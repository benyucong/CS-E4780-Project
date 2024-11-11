def query2_calculation(ema_38, ema_100, previous_ema_38, previous_ema_100):
    print(f"EMA_38: {ema_38}, EMA_100: {ema_100}, previous_EMA_38: {previous_ema_38}, previous_EMA_100: {previous_ema_100}")
    if ema_38 > ema_100 and previous_ema_38 <= previous_ema_100:
        print(f"Buy Detected")
        exit()
    elif ema_38 < ema_100 and previous_ema_38 >= previous_ema_100:
        print(f"Sell Detected")
        exit()
    else:
        print(f"No action")