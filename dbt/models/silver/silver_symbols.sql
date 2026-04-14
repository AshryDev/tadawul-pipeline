{{
    config(
        materialized = 'table',
        schema = 'silver',
    )
}}

/*
  silver_symbols
  ──────────────
  Static dimension table for the 10 tracked Tadawul symbols.
  Materialized as a full table (not incremental) since the symbol universe
  changes rarely and this is the source of truth for sector mappings.
*/

SELECT
    symbol,
    company_name,
    sector,
    market_cap_tier
FROM (
    VALUES
        ('2222', 'Saudi Aramco',    'Energy',          'Large'),
        ('1010', 'Al Rajhi Bank',   'Banking',         'Large'),
        ('2010', 'SABIC',           'Petrochemicals',  'Large'),
        ('7010', 'STC',             'Telecom',         'Large'),
        ('1120', 'Al Jazira Bank',  'Banking',         'Mid'),
        ('4280', 'Jarir Bookstore', 'Retail',          'Mid'),
        ('2380', 'Petro Rabigh',    'Petrochemicals',  'Mid'),
        ('8010', 'SABB',            'Banking',         'Large'),
        ('4003', 'Bahri',           'Transportation',  'Mid'),
        ('2060', 'Saudi Kayan',     'Petrochemicals',  'Mid')
) AS t (symbol, company_name, sector, market_cap_tier)
