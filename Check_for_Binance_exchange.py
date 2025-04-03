import ccxt

# Initialize Binance with Futures (USDT-M)
exchange = ccxt.binance({
    'enableRateLimit': True,
    'options': {
        'defaultType': 'future'  # USDT-M Futures
    }
})

# Load all markets
markets = exchange.load_markets()

# Filter for USDT perpetual futures (no options, no COIN-M, no delivery futures)
usdt_perpetual = {
    symbol: market for symbol, market in markets.items()
    if (
        market.get('quote') == 'USDT' and
        market.get('contract') is True and
        market.get('option') is False and
        market.get('expiry') is None and
        market.get('linear') is True
    )
}

# Print total
print(f"\n Total USDT Perpetual Futures on Binance: {len(usdt_perpetual)}\n")

# Fetch tickers once
tickers = exchange.fetch_tickers(list(usdt_perpetual.keys()))

# Print each symbol with current price
for symbol in usdt_perpetual.keys():
    price = tickers.get(symbol, {}).get('last')
    if price is not None:
        print(f"{symbol} -> {price}")
    else:
        print(f"{symbol} -> price not available")
