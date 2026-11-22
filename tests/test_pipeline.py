import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'fraud_detector' / 'src'))
# В Airflow этот каталог монтируется отдельно.
sys.path.insert(0, '/opt/airflow/fraud_src')
from preprocessing import run_preproc
from pipeline.retrain.evaluate_model import is_better
from pipeline.retrain.publish_model import publish_model
from pipeline.supply.sample_next_month import shift_into_month


class PipelineTests(unittest.TestCase):
    def test_same_features_for_training_and_stream(self):
        row = dict(merch=' Shop ', cat_id='Food', gender=None, one_city='City',
                   us_state='NY', jobs='Engineer', amount=100, population_city=1000,
                   lat=40, lon=-74, merchant_lat=40.1, merchant_lon=-74.1,
                   transaction_time='2020-01-01 23:59', target=1)
        raw = pd.DataFrame([row])
        trained = run_preproc(raw)
        online = run_preproc(raw.drop(columns='target'))
        pd.testing.assert_frame_equal(trained.drop(columns='target'), online)
        self.assertEqual(online.loc[0, 'gender'], 'unknown')
        self.assertTrue(np.isfinite(online.select_dtypes('number')).all().all())
        self.assertNotIn('transaction_time', online)
        self.assertNotIn('target', online)
        self.assertEqual(raw.loc[0, 'merch'], ' Shop ')

    def test_shift_end_of_month(self):
        frame = pd.DataFrame({'transaction_time': ['2020-01-31 12:34', '2020-01-01 00:00']})
        shifted = shift_into_month(frame, pd.Timestamp('2020-02-01'))
        self.assertEqual(str(shifted.transaction_time.iloc[0]), '2020-02-29 12:34:00')
        self.assertEqual(frame.transaction_time.iloc[0], '2020-01-31 12:34')

    def test_gate_rejects_regression_and_equal_model(self):
        old = {'roc_auc': .9, 'f1': .7}
        self.assertFalse(is_better({'roc_auc': .89, 'f1': .8}, old))
        self.assertFalse(is_better({'roc_auc': .91, 'f1': .69}, old))
        self.assertFalse(is_better(old, old))
        self.assertTrue(is_better({'roc_auc': .91, 'f1': .7}, old))

    def test_rejected_model_does_not_change_production_or_state(self):
        with tempfile.TemporaryDirectory() as folder:
            models = Path(folder)
            (models / 'latest.txt').write_text('initial\n')
            info = {'version': 'candidate', 'approved': False,
                    'candidate_metrics': {'roc_auc': .8}, 'current_metrics': {'roc_auc': .9}}
            with patch('pipeline.retrain.publish_model.MODELS_DIR', models), \
                 patch('pipeline.retrain.publish_model.get_conn') as connect:
                self.assertEqual(publish_model(info), 'rejected:candidate')
                connect.assert_not_called()
            self.assertEqual((models / 'latest.txt').read_text(), 'initial\n')

    def test_model_reloads_without_process_restart(self):
        from catboost import CatBoostClassifier
        import scorer
        data = pd.DataFrame({'amount': [1, 2, 10, 20]})
        with tempfile.TemporaryDirectory() as folder:
            models = Path(folder)
            for version, labels in [('initial', [0, 0, 1, 1]), ('new', [1, 1, 0, 0])]:
                model = CatBoostClassifier(iterations=5, depth=2, thread_count=1,
                                           allow_writing_files=False, verbose=False)
                model.fit(data, labels)
                model.save_model(str(models / f'my_catboost_{version}.cbm'))
            (models / 'latest.txt').write_text('initial\n')
            with patch.object(scorer, 'MODELS_DIR', models), \
                 patch.object(scorer, '_model', None), patch.object(scorer, '_version', None):
                _, first, version = scorer.make_pred(data)
                self.assertEqual(version, 'initial')
                (models / 'new.txt').write_text('new\n')
                (models / 'new.txt').replace(models / 'latest.txt')
                _, second, version = scorer.make_pred(data)
                self.assertEqual(version, 'new')
                self.assertFalse(np.allclose(first, second))

    def test_dags_have_schedule_and_dependencies(self):
        from airflow.models import DagBag
        bag = DagBag(dag_folder='/opt/airflow/dags', include_examples=False)
        self.assertEqual(bag.import_errors, {})
        for name, tasks in [('retrain_catboost', 7), ('supply_labeled_batch', 3), ('data_quality', 3)]:
            dag = bag.get_dag(name)
            self.assertIsNotNone(dag)
            self.assertEqual(len(dag.tasks), tasks)
            self.assertFalse(dag.catchup)
            self.assertEqual(dag.schedule_interval, '*/5 * * * *')
        self.assertEqual(bag.get_dag('retrain_catboost').get_task('publish_model').upstream_task_ids,
                         {'evaluate_model'})


if __name__ == '__main__':
    unittest.main()
