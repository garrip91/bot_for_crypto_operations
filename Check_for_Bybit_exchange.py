import ccxt

# Initialize Bybit exchange
exchange = ccxt.bybit({
    'enableRateLimit': True,
    'options': {
        'defaultType': 'linear'  # USDT Perpetual Futures
    }
})

# Load markets
markets = exchange.load_markets()

# Filter for USDT perpetual futures
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
print(f"\n Total USDT Perpetual Futures on Bybit: {len(usdt_perpetual)}\n")

# Fetch tickers once
tickers = exchange.fetch_tickers(list(usdt_perpetual.keys()))

# Print each symbol with its current price
for symbol in usdt_perpetual.keys():
    price = tickers.get(symbol, {}).get('last')
    if price is not None:
        print(f"{symbol} -> {price}")
    else:
        print(f"{symbol} -> price not available")
