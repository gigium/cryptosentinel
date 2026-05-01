-- Panel 1: Price Tracker (last 24h, all top-10 coins)
-- Line chart: x=ingested_at, y=current_price, series=coin_id
SELECT
    coin_id,
    symbol,
    ingested_at,
    current_price
FROM gold.crypto_features
WHERE ingested_at >= NOW() - INTERVAL 24 HOURS
ORDER BY ingested_at ASC;

-- ─────────────────────────────────────────────────────────────────────────────

-- Panel 2: Volume Spike Heatmap (latest vol_spike_ratio per coin per window)
-- Table with conditional formatting: amber > 2.5×, red > 5×
SELECT
    coin_id,
    symbol,
    MAX(CASE WHEN ingested_at >= NOW() - INTERVAL 1 HOUR  THEN vol_spike_ratio END) AS spike_1h,
    MAX(CASE WHEN ingested_at >= NOW() - INTERVAL 4 HOURS THEN vol_spike_ratio END) AS spike_4h,
    MAX(CASE WHEN ingested_at >= NOW() - INTERVAL 24 HOURS THEN vol_spike_ratio END) AS spike_24h
FROM gold.crypto_features
WHERE ingested_at >= NOW() - INTERVAL 24 HOURS
GROUP BY coin_id, symbol
ORDER BY spike_1h DESC NULLS LAST;

-- ─────────────────────────────────────────────────────────────────────────────

-- Panel 3: Market Cap Dominance (BTC & ETH, last 7 days)
-- Bar chart: x=ingested_at, y=mcap_dominance_pct, series=coin_id
SELECT
    coin_id,
    symbol,
    DATE_TRUNC('hour', ingested_at) AS hour,
    AVG(mcap_dominance_pct) AS avg_dominance_pct
FROM gold.crypto_features
WHERE
    coin_id IN ('bitcoin', 'ethereum')
    AND ingested_at >= NOW() - INTERVAL 7 DAYS
GROUP BY coin_id, symbol, DATE_TRUNC('hour', ingested_at)
ORDER BY hour ASC;

-- ─────────────────────────────────────────────────────────────────────────────

-- Panel 4: Spike Alert Table (vol_spike_ratio > 2.5, most recent first)
SELECT
    coin_id,
    symbol,
    ingested_at,
    vol_spike_ratio,
    ROUND(price_change_percentage_24h, 2)   AS price_delta_pct_24h,
    current_price,
    rsi_14
FROM gold.crypto_features
WHERE
    vol_spike_ratio > 2.5
    AND ingested_at >= NOW() - INTERVAL 24 HOURS
ORDER BY ingested_at DESC, vol_spike_ratio DESC
LIMIT 100;
