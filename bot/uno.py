import numpy as np
import scipy.stats as si
from scipy.optimize import newton


def BS_call(S, K, T, r, q, sigma):
    # S: spot price
    # K: strike price
    # T: time to maturity
    # r: interest rate
    # q: rate of continuous dividend paying asset
    # sigma: volatility of underlying asset

    d1 = (np.log(S / K) + (r - q + 0.5 * sigma ** 2) * T) / (sigma * np.sqrt(T))
    d2 = d1 - (sigma * np.sqrt(T))

    call = (S * np.exp(-q * T) * si.norm.cdf(d1, 0.0, 1.0)) - (K * np.exp(-r * T) * si.norm.cdf(d2, 0.0, 1.0))

    return call


def BS_put(S, K, T, r, q, sigma):
    # S: spot price
    # K: strike price
    # T: time to maturity
    # r: interest rate
    # q: rate of continuous dividend paying asset
    # sigma: volatility of underlying asset

    d1 = (np.log(S / K) + (r - q + 0.5 * sigma ** 2) * T) / (sigma * np.sqrt(T))
    d2 = d1 - (sigma * np.sqrt(T))

    put = (K * np.exp(-r * T) * si.norm.cdf(-d2, 0.0, 1.0)) - (S * np.exp(-q * T) * si.norm.cdf(-d1, 0.0, 1.0))

    return put


def delta_to_strikes(S, d, T, r, q, sigma):
    # S: spot price
    # d: delta
    # T: time to maturity
    # r: interest rate
    # q: rate of continuous dividend paying asset
    # sigma: volatility of underlying asset

    d1 = si.norm.ppf(d) * (sigma * np.sqrt(T)) - (r - q + 0.5 * sigma ** 2) * T
    strike = S / np.exp(d1)

    return strike


def bs_call_delta(S, K, T, r, q, sigma):
    # S: spot price
    # d: delta
    # T: time to maturity
    # r: interest rate
    # q: rate of continuous dividend paying asset
    # sigma: volatility of underlying asset

    d1 = (np.log(S / K) + (r - q + 0.5 * sigma ** 2) * T) / (sigma * np.sqrt(T))
    delta = si.norm.cdf(d1)

    return delta


def delta_vol(delta, t, atmv_0, atmv_1Y, atmv_inf, term_alpha, skew_1m, skew_inf, curv_1m, curv_inf):
    # t = days to maturity / 365

    # term structure
    if t> 0.05:
        st_vol = (atmv_0 - atmv_inf) * (1 - np.exp(-term_alpha * t)) / (term_alpha * t)
        #print(st_vol)
        lt_vol = np.exp(-term_alpha * t) * (atmv_1Y - atmv_inf)
        atm_vol = atmv_inf + st_vol - lt_vol
    else: # for really small t use a hack
        atmv_0 * (1 - t/ .08) + (atmv_1Y * t/ .08)
        atm_vol = atmv_0 * (1 - t/ .08) + atmv_1Y * (t/.08) # 0.05 / 0.08 so use some atmv_1Y

    # find the skew + curvature
    t_ratio = np.sqrt(1 / (t * 12))
    if t_ratio > 1.5: t_ratio = 1.5 # limit short term skew to 1.5 X skew_1m - 0.5 X skew_inf
    skew = skew_1m * t_ratio + skew_inf * (1 - t_ratio)
    if t_ratio > 1: t_ratio = 1 # limit to curv => curv_1m
    curv = curv_1m * t_ratio + curv_inf * (1 - t_ratio)

    vol = atm_vol * (1 + (skew * (delta * 100 - 50) / 1000) + (curv * ((delta * 100 - 50) ** 2) / 2000))
    if vol<0: vol = 0.01 # don't return NEGATIVE vols

    return vol


def find_vol(moneyness, expiry_days_as_fraction, atmv_0, atmv_1Y, atmv_inf, term_alpha, skew_1m, skew_inf, curv_1m,
             curv_inf, r, q):
    # input MONEYNESS - not BS forwards OR price strikes
    moneyness = moneyness * 100 # USE 100 BASE!!!

    def strike_delta(delta):

        vol = delta_vol(delta, expiry_days_as_fraction, atmv_0, atmv_1Y, atmv_inf, term_alpha, skew_1m, skew_inf,
                        curv_1m, curv_inf)
        #print(vol)
        K = delta_to_strikes(100, delta, expiry_days_as_fraction, r, q, vol)
        #print(K)
        return K - moneyness

    # find a vol for the strike
    try:
        final_delta = newton(strike_delta, 0.5, maxiter=1000)
        #print(final_delta)
    except RuntimeError as E:
        # try starting a seed at 0.9 or 0.1 delta
        try:
            final_delta = newton(strike_delta, x0=.9, maxiter=1000)
        except RuntimeError as E:
            try:
                final_delta = newton(strike_delta, x0=.1, maxiter=1000)
            except RuntimeError as E:
                # if still not converging, just take the zero or one delta vol
                if moneyness * ((expiry_days_as_fraction * (r - q)) + 1) > 100:
                    # if BS forward is greater than 100 - means higher strike
                    final_delta = 0.01
                else:
                    final_delta = 0.99

    final_delta = min(max(final_delta, 0.01), .99)

    final_vol = delta_vol(final_delta, expiry_days_as_fraction, atmv_0, atmv_1Y, atmv_inf, term_alpha, skew_1m,
                          skew_inf, curv_1m, curv_inf)

    final_vol = max(final_vol, 0.1)

    return final_vol


def convert_ORATS_vol(iv30, iv60, iv90, M3ATM, M4ATM):
    # convert ORATs market term structure to ARYA term structure
    iv30 = iv30 / 100
    iv60 = iv60 / 100
    iv90 = iv90 / 100
    M3ATM = M3ATM / 100
    M4ATM = M4ATM / 100

    one_mon_v = iv30

    term_vol = ((M4ATM - M3ATM) + (iv90 - iv60)) / 2
    one_year_v = (M4ATM + iv90) / 2 + term_vol * 0.75

    inf_vol = one_year_v + term_vol * 0.25
    spot_vol = (one_mon_v - inf_vol + 0.38 * (one_year_v - inf_vol)) / .62 + inf_vol

    return spot_vol, one_year_v, inf_vol


def Up_Out_Call(S, K, Bar, Reb, T, r, q, v1, v2):
    #   S- Spot, K -Strike, Bar - Barrier
    #   v1 is strike vol, #v2 is BARRIER vol - price with Reb = Bar - K
    v3 = (v1 + v2) / 2
    phi = 1
    eta = -1
    mu1 = (r - q - (v1 ** 2) / 2) / (v1 ** 2)
    mu2 = (r - q - (v2 ** 2) / 2) / (v2 ** 2)
    mu3 = (r - q - (v3 ** 2) / 2) / (v3 ** 2)
    lmbda = np.sqrt((mu2 ** 2) + (2 * r) / (v2 ** 2))  # only used for F (one Touch Barrier)
    z = np.log(Bar / S) / (v2 * np.sqrt(T)) + (lmbda * v2 * np.sqrt(T))  # only used for F (one Touch Barrier)

    x1 = np.log(S / K) / (v1 * np.sqrt(T)) + ((1 + mu1) * v1 * np.sqrt(T))  # call option with strike
    x2 = np.log(S / Bar) / (v2 * np.sqrt(T)) + ((1 + mu2) * v2 * np.sqrt(T))  # barrier
    y1 = np.log((Bar ** 2) / (S * K)) / (v3 * np.sqrt(T)) + ((1 + mu3) * v3 * np.sqrt(T))  # both
    y2 = np.log(Bar / S) / (v2 * np.sqrt(T)) + ((1 + mu2) * v2 * np.sqrt(T))  # barrier

    # A:v1, B:v2, C:v3, D:v2, F:v2
    A = phi * S * np.exp(-q * T) * si.norm.cdf(phi * x1) - phi * K * np.exp(-r * T) * si.norm.cdf(
        phi * x1 - phi * v1 * np.sqrt(T))  # call option with strike
    B = phi * S * np.exp(-q * T) * si.norm.cdf(phi * x2) - phi * K * np.exp(-r * T) * si.norm.cdf(
        phi * x2 - phi * v2 * np.sqrt(T))  # barrier
    C = phi * S * np.exp(-q * T) * (Bar / S) ** (2 * mu3 + 2) * si.norm.cdf(eta * y1) - phi * K * np.exp(-r * T) * (
                Bar / S) ** (2 * mu3) * si.norm.cdf(eta * y1 - eta * v3 * np.sqrt(T))
    D = phi * S * np.exp(-q * T) * (Bar / S) ** (2 * mu2 + 2) * si.norm.cdf(eta * y2) - phi * K * np.exp(-r * T) * (
                Bar / S) ** (2 * mu2) * si.norm.cdf(eta * y2 - eta * v2 * np.sqrt(T))
    F = Reb * (((Bar / S) ** (mu2 + lmbda)) * si.norm.cdf(eta * z) + ((Bar / S) ** (mu2 - lmbda)) * si.norm.cdf(
        eta * z - 2 * eta * lmbda * v2 * np.sqrt(T)))

    UnOC = A - B + C - D + F

    return UnOC


def One_T_Bar(S, K, Bar, Reb, T, r, q, v):
    # ONLY use for pricing the one touch rebate
    eta = -1
    mu = (r - q - (v ** 2) / 2) / (v ** 2)
    lmbda = np.sqrt((mu ** 2) + (2 * r) / (v ** 2))
    z = np.log(Bar / S) / (v * np.sqrt(T)) + (lmbda * v * np.sqrt(T))

    F = Reb * (((Bar / S) ** (mu + lmbda)) * si.norm.cdf(eta * z) + ((Bar / S) ** (mu - lmbda)) * si.norm.cdf(
        eta * z - 2 * eta * lmbda * v * np.sqrt(T)))

    return F


def deltaUnOC(S, K, Bar, Reb, T, r, q, v1, v2):
    p1 = Up_Out_Call(S * 1.01, K, Bar, Reb, T, r, q, v1, v2)
    p2 = Up_Out_Call(S * .99, K, Bar, Reb, T, r, q, v1, v2)

    delta = (p1 - p2) / (S * .02)

    return delta


def Rev_Conv(S, K1, K2, T, r, q, v1, v2):
    p1 = BS_put(S, K1, T, r, q, v1)
    p2 = BS_put(S, K2, T, r, q, v2)

    rev_conv = p2 - p1

    return rev_conv

def deltaRC(S, K1, K2, T, r, q, v1, v2):
    #delta of up and out KO call
    p1 = Rev_Conv(S * 1.01, K1, K2, T, r, q, v1, v2)
    p2 = Rev_Conv(S * .99, K1, K2, T, r, q, v1, v2)

    delta = (p1 - p2) / (S * .02)

    return delta

# if __name__ == "__main__":
#     #v0 = uno.find_vol(1, t, atm_volatility_spot, atm_volatility_one_year,atm_volatility_infinity, 12, slope, slope_inf, deriv, deriv_inf, r, q)
    
#     #MSFT.O
#     MSFT = find_vol(1, 0.0821917808219178, 0.357950403225807, 0.3640875,0.3599, 12, 2, 2, 0.091, 0.0692, 0.0015, 0)
#     print(MSFT)

#     AAPL = find_vol(1, 0.0821917808219178, 0.5213088709677419, 0.440825,0.43395, 12, 1, 0, 0.0105, 0, 0.0015, 0)
#     print(AAPL)