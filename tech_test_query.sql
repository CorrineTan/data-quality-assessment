-- Generate a date series from June 1, 2020, to September 30, 2020
WITH date_series AS (
    SELECT generate_series('2020-06-01'::date, '2020-09-30'::date, '1 day') AS dt_report
),

-- Get enabled users
enabled_users AS (
    SELECT login_hash
    FROM users
    WHERE enable = TRUE
),

-- Base data combining date series with distinct login/server/symbol combinations
base_data AS (
    SELECT
        ds.dt_report,
        u.login_hash,
        t.server_hash,
        t.symbol
    FROM date_series ds
    CROSS JOIN enabled_users u
    CROSS JOIN (
        SELECT DISTINCT server_hash, symbol
        FROM trades
    ) t
),

-- Filtered trades fixing quality issues
filtered_trades AS (
    SELECT *
    FROM trades
    WHERE
        volume >= 0 -- Exclude negative volumes
        AND open_price >= 0 -- Exclude negative prices
        AND cmd IN (0, 1) -- Valid cmd values
        AND login_hash IN (SELECT login_hash FROM enabled_users) -- Only enabled users
),

-- Aggregated trade data
aggregated_trades AS (
    SELECT
        login_hash,
        server_hash,
        symbol,
        open_time::date AS trade_date,
        volume,
        COUNT(ticket_hash) AS trade_count
    FROM filtered_trades
    GROUP BY login_hash, server_hash, symbol, open_time::date, volume
)

SELECT
    ROW_NUMBER() OVER (ORDER BY dt_report DESC, login_hash, server_hash, symbol) AS id,
    bd.dt_report,
    bd.login_hash,
    bd.server_hash,
    bd.symbol,
    s.currency,
    
    -- Sum of volume traded in previous 7 days including current dt_report
    (
        SELECT COALESCE(SUM(volume), 0)
        FROM filtered_trades ft
        WHERE
            ft.login_hash = bd.login_hash
            AND ft.server_hash = bd.server_hash
            AND ft.symbol = bd.symbol
            AND ft.open_time::date BETWEEN bd.dt_report - INTERVAL '6 days' AND bd.dt_report
    ) AS sum_volume_prev_7d,
    
    -- Sum of all volume traded up to current dt_report
    (
        SELECT COALESCE(SUM(volume), 0)
        FROM filtered_trades ft
        WHERE
            ft.login_hash = bd.login_hash
            AND ft.server_hash = bd.server_hash
            AND ft.symbol = bd.symbol
            AND ft.open_time::date <= bd.dt_report
    ) AS sum_volume_prev_all,
    
    -- Rank of most volume traded by login/symbol in previous 7 days
    DENSE_RANK() OVER (
        PARTITION BY bd.login_hash, bd.symbol
        ORDER BY
            (
                SELECT COALESCE(SUM(volume), 0)
                FROM filtered_trades ft
                WHERE
                    ft.login_hash = bd.login_hash
                    AND ft.symbol = bd.symbol
                    AND ft.open_time::date BETWEEN bd.dt_report - INTERVAL '6 days' AND bd.dt_report
            ) DESC
    ) AS rank_volume_symbol_prev_7d,
    
    -- Rank of most trade count by login in previous 7 days
    DENSE_RANK() OVER (
        PARTITION BY bd.login_hash
        ORDER BY
            (
                SELECT COALESCE(COUNT(ticket_hash), 0)
                FROM filtered_trades ft
                WHERE
                    ft.login_hash = bd.login_hash
                    AND ft.open_time::date BETWEEN bd.dt_report - INTERVAL '6 days' AND bd.dt_report
            ) DESC
    ) AS rank_count_prev_7d,
    
    -- Sum of volume traded in August 2020 up to current dt_report
    (
        SELECT COALESCE(SUM(volume), 0)
        FROM filtered_trades ft
        WHERE
            ft.login_hash = bd.login_hash
            AND ft.server_hash = bd.server_hash
            AND ft.symbol = bd.symbol
            AND ft.open_time::date BETWEEN '2020-08-01' AND bd.dt_report
    ) AS sum_volume_2020_08,
    
    -- Date of first trade up to current dt_report
    (
        SELECT MIN(open_time)
        FROM filtered_trades ft
        WHERE
            ft.login_hash = bd.login_hash
            AND ft.server_hash = bd.server_hash
            AND ft.symbol = bd.symbol
            AND ft.open_time::date <= bd.dt_report
    ) AS date_first_trade,
    
    -- Row number ordered by dt_report/login/server/symbol
    ROW_NUMBER() OVER (ORDER BY dt_report DESC, login_hash, server_hash, symbol) AS row_number

FROM base_data bd
LEFT JOIN symbols s ON bd.symbol = s.symbol

ORDER BY row_number DESC;
