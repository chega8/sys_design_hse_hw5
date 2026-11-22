"""Проверка повторов в отдельной временной схеме PostgreSQL."""
import os
import tempfile
import unittest
import uuid
from datetime import date
from pathlib import Path
from unittest.mock import patch

import pandas as pd
from pipeline.db import get_conn, read_training_state
from pipeline.retrain.publish_model import publish_model
from pipeline.supply.append_labeled_batch import append_labeled_batch


@unittest.skipUnless(os.getenv('TEST_DATABASE') == '1', 'Нужна запущенная PostgreSQL: TEST_DATABASE=1')
class RetryTests(unittest.TestCase):
    def setUp(self):
        self.schema = 'test_retry_' + uuid.uuid4().hex
        conn = get_conn()
        with conn:
            with conn.cursor() as cur:
                cur.execute(f'CREATE SCHEMA {self.schema}')
                cur.execute(f'CREATE TABLE {self.schema}.labeled_transactions (LIKE public.labeled_transactions INCLUDING ALL)')
                cur.execute(f'CREATE TABLE {self.schema}.training_state (LIKE public.training_state INCLUDING ALL)')
                cur.execute(f"INSERT INTO {self.schema}.training_state (id, window_start, window_end, next_batch_month) VALUES (1, '2019-01-01', '2019-03-01', '2019-04-01')")
        conn.close()
        self.options = patch.dict(os.environ, {'PGOPTIONS': '-c search_path=' + self.schema})
        self.options.start()
        self.folder = tempfile.TemporaryDirectory()
        self.root = Path(self.folder.name)

    def tearDown(self):
        self.options.stop()
        conn = get_conn()
        with conn:
            with conn.cursor() as cur:
                cur.execute(f'DROP SCHEMA {self.schema} CASCADE')
        conn.close()
        self.folder.cleanup()

    def state(self):
        conn = get_conn()
        try:
            return read_training_state(conn)
        finally:
            conn.close()

    def test_publish_retry_after_pointer_written(self):
        (self.root / 'latest.txt').write_text('initial\n')
        staging = self.root / 'candidate.cbm'
        staging.write_bytes(b'test-model')
        pending = self.root / 'pending.parquet'
        pd.DataFrame({'amount': [1]}).to_parquet(pending)
        info = dict(version='test', approved=True, staging_model=str(staging),
                    dataset_path=str(pending), batch_month='2019-04-01',
                    train_months='2019-02,2019-03', test_month='2019-04',
                    train_rows=2, test_rows=1, roc_auc=.9, f1=.7)
        with patch('pipeline.retrain.publish_model.MODELS_DIR', self.root), \
             patch('pipeline.retrain.publish_model.CURRENT_DATASET', self.root / 'current.parquet'):
            # Сбой после публикации файла, до commit указателя в БД.
            with patch('pipeline.retrain.publish_model.advance_training_state', side_effect=RuntimeError('crash')):
                with self.assertRaises(RuntimeError):
                    publish_model(info)
            self.assertEqual(self.state()['next_batch_month'], date(2019, 4, 1))
            publish_model(info)
            publish_model(info)
        self.assertEqual(self.state()['next_batch_month'], date(2019, 5, 1))
        self.assertEqual((self.root / 'latest.txt').read_text(), 'test\n')

    def test_append_retry_after_commit(self):
        conn = get_conn()
        frame = pd.read_sql('SELECT * FROM public.labeled_transactions LIMIT 4', conn)
        conn.close()
        frame['transaction_time'] = pd.Timestamp('2020-04-15 12:00')
        path = self.root / 'supply.parquet'
        frame.to_parquet(path, index=False)
        info = dict(batch_path=str(path), batch_month='2020-04-01')
        append_labeled_batch(info)
        append_labeled_batch(info)
        conn = get_conn()
        with conn.cursor() as cur:
            cur.execute('SELECT count(*) FROM labeled_transactions')
            self.assertEqual(cur.fetchone()[0], 4)
            cur.execute('SELECT count(*) FROM supplied_months')
            self.assertEqual(cur.fetchone()[0], 1)
        conn.close()

    def test_bad_data_still_produces_html(self):
        from pipeline.data_quality import check_dataset, write_report, require_quality
        conn = get_conn()
        with conn:
            with conn.cursor() as cur:
                cur.execute('INSERT INTO labeled_transactions SELECT * FROM public.labeled_transactions LIMIT 1')
                cur.execute('UPDATE labeled_transactions SET amount = -1, target = 7, lat = 999')
        conn.close()
        result = check_dataset()
        with patch('pipeline.data_quality.REPORTS', self.root):
            write_report(result)
        with self.assertRaises(ValueError):
            require_quality(result)
        self.assertIn('FAIL', (self.root / 'latest.html').read_text())
        self.assertEqual(len(result['checks']), 9)

    def test_window_retry_uses_calendar_bounds(self):
        from pipeline.retrain.slide_training_window import slide_training_window
        from pipeline.retrain.time_train_test_split import time_train_test_split
        from pipeline.db import copy_labeled_frame
        conn = get_conn()
        source = pd.read_sql('SELECT * FROM public.labeled_transactions LIMIT 1', conn)
        for month in range(1, 5):
            frame = source.copy()
            frame['transaction_time'] = pd.Timestamp(2019, month, 15)
            copy_labeled_frame(conn, frame)
        conn.close()
        with patch('pipeline.retrain.slide_training_window.PENDING_WINDOW', self.root / 'pending.parquet'), \
             patch('pipeline.retrain.time_train_test_split.SPLITS_DIR', self.root):
            info = slide_training_window({'batch_month': '2019-04-01'})
            first = pd.read_parquet(info['dataset_path'])
            info = slide_training_window({'batch_month': '2019-04-01'})
            pd.testing.assert_frame_equal(first, pd.read_parquet(info['dataset_path']))
            split = time_train_test_split(info)
        self.assertEqual(len(first), 3)
        self.assertEqual(split['train_months'], '2019-02,2019-03')
        self.assertEqual(split['test_month'], '2019-04')

    def test_retention_keeps_boundary_and_can_repeat(self):
        from pipeline.db import copy_labeled_frame
        from pipeline.supply.prune_old_months import prune_old_months
        conn = get_conn()
        source = pd.read_sql('SELECT * FROM public.labeled_transactions LIMIT 1', conn)
        for time in ['2019-04-01', '2020-03-31 23:59', '2020-04-01', '2023-04-30']:
            frame = source.copy()
            frame['transaction_time'] = pd.Timestamp(time)
            copy_labeled_frame(conn, frame)
        conn.close()
        result = prune_old_months({'batch_month': '2023-04-01'})
        self.assertEqual(result['cutoff'], '2020-04-01')
        self.assertEqual(result['deleted_rows'], 2)
        self.assertEqual(prune_old_months({'batch_month': '2023-04-01'})['deleted_rows'], 0)
