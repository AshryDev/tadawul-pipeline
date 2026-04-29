"""
Canonical list of Tadawul-listed symbols for all pipeline components.

Symbols are the 4-digit numeric codes used by the Saudi Exchange.
Append '.SR' for Yahoo Finance (e.g. '2222.SR' for Saudi Aramco).

Override the full list at runtime by setting the SYMBOLS env var to a
comma-separated list: SYMBOLS=2222,1010,2010
"""
from __future__ import annotations

import os

TADAWUL_SYMBOLS: list[str] = [
    # ── Energy ────────────────────────────────────────────────────────────────
    "2222",  # Saudi Aramco
    "5110",  # ACWA Power
    "5170",  # Saudi Electricity Company

    # ── Mining ────────────────────────────────────────────────────────────────
    "1211",  # Ma'aden (Saudi Arabian Mining Company)

    # ── Petrochemicals & Basic Materials ─────────────────────────────────────
    "2010",  # SABIC
    "2030",  # Tasnee (National Industrialization)
    "2060",  # Saudi Kayan
    "2080",  # Sahara International Petrochemical
    "2090",  # National Metal Manufacturing & Casting
    "2160",  # National Gypsum
    "2170",  # Saudi Steel Pipes
    "2180",  # Astra Industrial
    "2200",  # Filling & Packing Materials Manufacturing
    "2210",  # Zamil Industrial
    "2220",  # Chemanol (Methanol Chemicals)
    "2240",  # Saudi Paper Manufacturing
    "2250",  # SAFCO
    "2290",  # YANSAB (Yanbu National Petrochemical)
    "2300",  # Saudi Pipe
    "2310",  # Sipchem
    "2320",  # Al Babtain Power & Telecommunication
    "2330",  # Advanced Petrochemical
    "2350",  # Saudi Ceramic
    "2380",  # Petro Rabigh

    # ── Banking & Finance ─────────────────────────────────────────────────────
    "1010",  # Al Rajhi Bank
    "1020",  # Riyad Bank
    "1030",  # Saudi British Bank (SABB)
    "1050",  # Banque Saudi Fransi
    "1060",  # Saudi Investment Bank
    "1080",  # Arab National Bank
    "1120",  # Bank AlJazira
    "1140",  # Bank Albilad
    "1150",  # Alinma Bank
    "1180",  # Saudi National Bank (SNB)

    # ── Capital Goods & Industrial ────────────────────────────────────────────
    "4010",  # Dallah Albaraka Holding
    "4020",  # National Gas & Industrialization (GASCO)
    "4230",  # Al Hassan Ghazi Ibrahim Shaker
    "4240",  # Saudi Industrial Services (SISCO)

    # ── Cement ────────────────────────────────────────────────────────────────
    "3010",  # Yamama Cement
    "3020",  # Saudi Cement
    "3030",  # Qassim Cement
    "3040",  # Southern Province Cement
    "3050",  # Yanbu Cement
    "3060",  # City Cement
    "3080",  # Tabuk Cement
    "3090",  # Arabian Cement

    # ── Retail & Consumer ────────────────────────────────────────────────────
    "4050",  # Savola Group
    "4160",  # Almarai
    "4200",  # Aldrees Petroleum
    "4210",  # Abdullah Al Othaim Markets
    "4280",  # Jarir Bookstore

    # ── Consumer Services ─────────────────────────────────────────────────────
    "4001",  # Arabian Centres (Cenomi Centers)
    "4007",  # Leejam Sports (Fitness Time)

    # ── Healthcare ────────────────────────────────────────────────────────────
    "4002",  # Dallah Healthcare
    "4005",  # Saudi Pharmaceutical Industries (SPIMACO)
    "4013",  # Dr. Sulaiman Al Habib Medical Services
    "4100",  # Mouwasat Medical Services
    "4130",  # Al Hammadi Medical

    # ── Food & Beverages ─────────────────────────────────────────────────────
    "6010",  # SADAFCO (Saudia Dairy)
    "6020",  # Halwani Brothers
    "6040",  # Herfy Food Services
    "6050",  # Saudi Fisheries
    "6070",  # Al-Jouf Agriculture Development

    # ── Telecom & Technology ─────────────────────────────────────────────────
    "7010",  # STC (Saudi Telecom Company)
    "7020",  # Mobily (Etihad Etisalat)
    "7030",  # Zain KSA
    "7040",  # Saudi Telecom subsidiary? Atheeb? Actually 7040 is STC Solutions

    # ── Insurance ────────────────────────────────────────────────────────────
    "8010",  # SABB Takaful
    "8020",  # Al-Sagr National Insurance
    "8050",  # Bupa Arabia
    "8060",  # Malath Insurance
    "8070",  # Najm for Insurance Services
    "8100",  # Tawuniya (Company for Cooperative Insurance)
    "8150",  # MedGulf Insurance
    "8160",  # Arabian Shield Cooperative Insurance
    "8180",  # Al Ahlia Cooperative Insurance
    "8200",  # Salama Cooperative Insurance
    "8230",  # Mediterranean & Gulf Insurance
    "8240",  # Al Rajhi Takaful
    "8260",  # Solidarity Saudi Takaful
    "8300",  # Walaa Cooperative Insurance

    # ── Transportation & Logistics ───────────────────────────────────────────
    "4003",  # Bahri (National Shipping)
    "4030",  # Saudi Airlines Catering
    "4031",  # Saudi Ground Services
    "4040",  # Al Tayyar Travel
    "4150",  # Saudia Catering

    # ── Real Estate & Construction ───────────────────────────────────────────
    "4006",  # Taiba Holding
    "4190",  # Jabal Omar Development
    "4250",  # Saudi Cable Company
    "4300",  # Dar Al Arkan Real Estate
    "4310",  # Emaar The Economic City
    "4320",  # Arriyadh Development
]

# Approximate base prices in SAR — used to seed the random-walk simulation.
# Unknown symbols default to 100.0 in the producer's simulate_tick().
BASE_PRICES: dict[str, float] = {
    # Energy
    "2222": 30.0,   # Saudi Aramco
    "5110": 72.0,   # ACWA Power
    "5170": 20.0,   # Saudi Electricity Company
    # Mining
    "1211": 55.0,   # Ma'aden
    # Petrochemicals
    "2010": 120.0,  # SABIC
    "2030": 18.0,   # Tasnee
    "2060": 15.0,   # Saudi Kayan
    "2080": 18.0,   # Sahara Petrochemical
    "2090": 22.0,   # National Metal Manufacturing
    "2160": 12.0,   # National Gypsum
    "2170": 48.0,   # Saudi Steel Pipes
    "2180": 55.0,   # Astra Industrial
    "2200": 30.0,   # Filling & Packing
    "2210": 38.0,   # Zamil Industrial
    "2220": 13.0,   # Chemanol
    "2240": 25.0,   # Saudi Paper Manufacturing
    "2250": 100.0,  # SAFCO
    "2290": 55.0,   # YANSAB
    "2300": 35.0,   # Saudi Pipe
    "2310": 28.0,   # Sipchem
    "2320": 60.0,   # Al Babtain
    "2330": 68.0,   # Advanced Petrochemical
    "2350": 22.0,   # Saudi Ceramic
    "2380": 25.0,   # Petro Rabigh
    # Banking
    "1010": 95.0,   # Al Rajhi Bank
    "1020": 28.0,   # Riyad Bank
    "1030": 42.0,   # Saudi British Bank
    "1050": 38.0,   # Banque Saudi Fransi
    "1060": 14.0,   # Saudi Investment Bank
    "1080": 32.0,   # Arab National Bank
    "1120": 16.0,   # Bank AlJazira
    "1140": 22.0,   # Bank Albilad
    "1150": 24.0,   # Alinma Bank
    "1180": 48.0,   # Saudi National Bank
    # Capital Goods
    "4010": 35.0,   # Dallah Albaraka
    "4020": 95.0,   # GASCO
    "4230": 42.0,   # Al Hassan Shaker
    "4240": 28.0,   # SISCO
    # Cement
    "3010": 28.0,   # Yamama Cement
    "3020": 85.0,   # Saudi Cement
    "3030": 75.0,   # Qassim Cement
    "3040": 70.0,   # Southern Province Cement
    "3050": 22.0,   # Yanbu Cement
    "3060": 22.0,   # City Cement
    "3080": 18.0,   # Tabuk Cement
    "3090": 35.0,   # Arabian Cement
    # Retail & Consumer
    "4050": 32.0,   # Savola Group
    "4160": 55.0,   # Almarai
    "4200": 80.0,   # Aldrees Petroleum
    "4210": 115.0,  # Abdullah Al Othaim Markets
    "4280": 185.0,  # Jarir Bookstore
    # Consumer Services
    "4001": 26.0,   # Arabian Centres
    "4007": 72.0,   # Leejam Sports
    # Healthcare
    "4002": 180.0,  # Dallah Healthcare
    "4005": 60.0,   # SPIMACO
    "4013": 320.0,  # Dr. Sulaiman Al Habib
    "4100": 135.0,  # Mouwasat Medical
    "4130": 48.0,   # Al Hammadi Medical
    # Food & Beverage
    "6010": 95.0,   # SADAFCO
    "6020": 115.0,  # Halwani Brothers
    "6040": 68.0,   # Herfy Food Services
    "6050": 28.0,   # Saudi Fisheries
    "6070": 40.0,   # Al-Jouf Agriculture
    # Telecom
    "7010": 55.0,   # STC
    "7020": 14.0,   # Mobily
    "7030": 11.0,   # Zain KSA
    "7040": 38.0,   # STC Solutions
    # Insurance
    "8010": 48.0,   # SABB Takaful
    "8020": 32.0,   # Al-Sagr National
    "8050": 145.0,  # Bupa Arabia
    "8060": 22.0,   # Malath Insurance
    "8070": 55.0,   # Najm
    "8100": 95.0,   # Tawuniya
    "8150": 30.0,   # MedGulf
    "8160": 28.0,   # Arabian Shield
    "8180": 38.0,   # Al Ahlia
    "8200": 18.0,   # Salama Cooperative
    "8230": 35.0,   # Mediterranean & Gulf
    "8240": 22.0,   # Al Rajhi Takaful
    "8260": 20.0,   # Solidarity Saudi Takaful
    "8300": 28.0,   # Walaa Cooperative
    # Transportation
    "4003": 40.0,   # Bahri
    "4030": 70.0,   # Saudi Airlines Catering
    "4031": 80.0,   # Saudi Ground Services
    "4040": 28.0,   # Al Tayyar Travel
    "4150": 55.0,   # Saudia Catering
    # Real Estate & Construction
    "4006": 12.0,   # Taiba Holding
    "4190": 36.0,   # Jabal Omar Development
    "4250": 35.0,   # Saudi Cable Company
    "4300": 22.0,   # Dar Al Arkan
    "4310": 12.0,   # Emaar The Economic City
    "4320": 18.0,   # Arriyadh Development
}


def get_symbols() -> list[str]:
    """
    Return the active symbol list.

    Reads SYMBOLS env var if set (comma-separated codes); otherwise returns
    the full TADAWUL_SYMBOLS list above.
    """
    env = os.getenv("SYMBOLS", "").strip()
    if env:
        return [s.strip() for s in env.split(",") if s.strip()]
    return list(TADAWUL_SYMBOLS)
