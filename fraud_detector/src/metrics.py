from prometheus_client import Counter, Gauge, Histogram

SCORE_BUCKETS = (0, .05, .1, .15, .2, .25, .3, .35, .4, .45, .5, .55, .6,
                 .65, .7, .75, .8, .85, .9, .95, 1)

SCORED = Counter("fraud_scored_total", "Transactions delivered to the scoring topic")
FLAGGED = Counter("fraud_flagged_total", "Transactions flagged by the model")
FRAUD_RATIO = Gauge("fraud_ratio", "Share of transactions flagged since startup")
ERRORS = Counter("fraud_scoring_errors_total", "Transactions that could not be scored")
DURATION = Histogram("fraud_processing_seconds", "Processing time including Kafka delivery")
PREPROCESSING = Histogram(
    "fraud_preprocessing_seconds", "Feature preparation time",
    buckets=(.001, .0025, .005, .01, .02, .05, .1, .25, .5, 1),
)
SCORES = Histogram("fraud_score", "Model scores", buckets=SCORE_BUCKETS)
MODEL_RELOADS = Counter("fraud_model_reloads_total", "Model updates", ["version"])
MODEL_RELOADED_AT = Gauge(
    "fraud_model_reloaded_at_seconds", "Unix time of the last load of this version", ["version"],
)
