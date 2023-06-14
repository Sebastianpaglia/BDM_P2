 CREATE TABLE IF NOT EXISTS model_metrics
(
    timestamp_column timestamp DEFAULT CURRENT_TIMESTAMP, model_type VARCHAR(255),
    r2 double precision,
    rmse double precision,
    mae double precision,
    PRIMARY KEY (timestamp_column)
);

 CREATE TABLE IF NOT EXISTS results_neighborhood (
    neighborhood VARCHAR(255),
    latitude DOUBLE PRECISION,
    longitude DOUBLE PRECISION,
    price_mean DOUBLE PRECISION,
    prediction_mean DOUBLE PRECISION,
    price_median DOUBLE PRECISION,
    prediction_median DOUBLE PRECISION,
    count_per_neighborhood BIGINT,
    timestamps_column TIMESTAMP DEFAULT CURRENT_TIMESTAMP, model_type VARCHAR(255),
    PRIMARY KEY (neighborhood, timestamps_column)
);