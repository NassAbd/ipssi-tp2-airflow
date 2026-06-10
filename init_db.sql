-- =========================================================================
-- Script SQL d'initialisation de la base de données PostgreSQL pour le TP5
-- =========================================================================

-- 1. Table de stockage des données de mesures météo (Propres et validées)
CREATE TABLE IF NOT EXISTS weather_measures (
    id SERIAL PRIMARY KEY,
    city VARCHAR(50) NOT NULL,
    temperature NUMERIC(4, 2) NOT NULL,
    conditions VARCHAR(100) NOT NULL,
    humidity INT NOT NULL,
    timestamp TIMESTAMP WITH TIME ZONE NOT NULL,
    CONSTRAINT unique_city_timestamp UNIQUE (city, timestamp)
);

-- 2. Table d'audit/suivi d'ingestion des exécutions du DAG
CREATE TABLE IF NOT EXISTS ingestion_runs (
    id SERIAL PRIMARY KEY,
    run_id VARCHAR(100) NOT NULL,
    execution_date TIMESTAMP WITH TIME ZONE NOT NULL,
    status VARCHAR(20) NOT NULL,
    cities_processed VARCHAR(255) NOT NULL,
    records_inserted INT NOT NULL,
    inserted_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP NOT NULL,
    CONSTRAINT unique_run_id UNIQUE (run_id)
);

-- 3. Table de suivi des anomalies de qualité des données météo
CREATE TABLE IF NOT EXISTS weather_anomalies (
    id SERIAL PRIMARY KEY,
    city VARCHAR(50) NOT NULL,
    metric VARCHAR(50) NOT NULL,
    value NUMERIC(6, 2) NOT NULL,
    reason VARCHAR(255) NOT NULL,
    run_id VARCHAR(100) NOT NULL,
    timestamp TIMESTAMP WITH TIME ZONE NOT NULL,
    inserted_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP NOT NULL
);
