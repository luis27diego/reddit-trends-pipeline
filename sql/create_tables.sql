-- ============================================================
-- SCRIPT FINAL CON id AUTOINCREMENTAL EN TODAS LAS TABLAS
-- Base de datos: reddit_analytics
-- Listo para copiar-pegar y ejecutar
-- ============================================================

-- 1. Tendencias diarias
DROP TABLE IF EXISTS tendencias_diarias CASCADE;
CREATE TABLE tendencias_diarias (
    id                  SERIAL PRIMARY KEY,
    periodo             DATE        NOT NULL,
    avg_sentiment       DOUBLE PRECISION,
    volumen             BIGINT
);

-- 2. Temas LDA
DROP TABLE IF EXISTS temas_lda CASCADE;
CREATE TABLE temas_lda (
    id                  SERIAL PRIMARY KEY,
    topic               INTEGER     NOT NULL,
    terms               TEXT[]      NOT NULL,
    termweights         DOUBLE PRECISION[] NOT NULL
);

-- 3. Validación sentimiento vs score
DROP TABLE IF EXISTS validacion_sentimiento CASCADE;
CREATE TABLE validacion_sentimiento (
    id                  SERIAL PRIMARY KEY,
    rango_score         TEXT        NOT NULL,
    avg_sentiment_real  DOUBLE PRECISION,
    desviacion_sentiment DOUBLE PRECISION
);

-- 4. Controversia por subreddit
DROP TABLE IF EXISTS controversia_por_subreddit CASCADE;
CREATE TABLE controversia_por_subreddit (
    id                  SERIAL PRIMARY KEY,
    subreddit_name      TEXT        NOT NULL,
    tipo_contenido      TEXT        NOT NULL,
    cantidad            BIGINT,
    score_promedio      DOUBLE PRECISION,
    sentiment_promedio  DOUBLE PRECISION
);

-- 5. Patrones horarios
DROP TABLE IF EXISTS patrones_horarios CASCADE;
CREATE TABLE patrones_horarios (
    id                  SERIAL PRIMARY KEY,
    dia_semana          INTEGER NOT NULL,
    nombre_dia          TEXT    NOT NULL,
    hora                INTEGER NOT NULL CHECK (hora BETWEEN 0 AND 23),
    score_promedio      DOUBLE PRECISION,
    volumen             BIGINT,
    sentiment_promedio  DOUBLE PRECISION
);

-- 6. Top palabras por sentimiento
DROP TABLE IF EXISTS top_palabras_sentimiento CASCADE;
CREATE TABLE top_palabras_sentimiento (
    id                  SERIAL PRIMARY KEY,
    sentiment_categoria TEXT    NOT NULL,
    word                TEXT    NOT NULL,
    frecuencia          BIGINT  NOT NULL,
    rank                INTEGER NOT NULL
);

-- 7. Anomalías / picos de actividad
DROP TABLE IF EXISTS anomalias CASCADE;
CREATE TABLE anomalias (
    id                  SERIAL PRIMARY KEY,
    periodo             TIMESTAMP   NOT NULL,
    volumen             BIGINT,
    sentiment_promedio  DOUBLE PRECISION,
    media_movil         DOUBLE PRECISION,
    std_movil           DOUBLE PRECISION,
    desviacion_std      DOUBLE PRECISION,
    es_anomalia         TEXT        NOT NULL
);

-- 8. Comparativa entre subreddits
DROP TABLE IF EXISTS comparativa_subreddits CASCADE;
CREATE TABLE comparativa_subreddits (
    id                  SERIAL PRIMARY KEY,
    subreddit_name      TEXT        NOT NULL,
    total_comentarios   BIGINT,
    sentiment_promedio  DOUBLE PRECISION,
    sentiment_std       DOUBLE PRECISION,
    score_promedio      DOUBLE PRECISION,
    contenido_nsfw      BIGINT,
    primer_comentario   TIMESTAMP,
    ultimo_comentario   TIMESTAMP,
    dias_activos        INTEGER,
    comentarios_por_dia DOUBLE PRECISION
);

-- 9. Distribución sentiment vs score
DROP TABLE IF EXISTS distribucion_sentiment_score CASCADE;
CREATE TABLE distribucion_sentiment_score (
    id                  SERIAL PRIMARY KEY,
    rango_score         TEXT    NOT NULL,
    rango_sentiment     TEXT    NOT NULL,
    cantidad            BIGINT
);

-- 10. Comentarios extremos
DROP TABLE IF EXISTS comentarios_extremos CASCADE;
CREATE TABLE comentarios_extremos (
    id                  SERIAL PRIMARY KEY,        -- ahora es SERIAL, no TEXT
    subreddit_name      TEXT,
    created_utc         BIGINT,
    score               BIGINT,
    sentiment           DOUBLE PRECISION,
    extremo_tipo        TEXT,
    preview_texto       TEXT
);

-- Índices útiles (actualizados con los nombres correctos que usas)
CREATE INDEX IF NOT EXISTS idx_anomalias_periodo ON anomalias (periodo DESC);
CREATE INDEX IF NOT EXISTS idx_tendencias_periodo ON tendencias_diarias (periodo DESC);
CREATE INDEX IF NOT EXISTS idx_comentarios_score ON comentarios_extremos (score DESC);
CREATE INDEX IF NOT EXISTS idx_palabras_sentimiento ON top_palabras_sentimiento (sentiment_categoria, frecuencia DESC);

-- Mensaje final
SELECT 'Todas las tablas creadas con columna id SERIAL autoincremental' AS status;