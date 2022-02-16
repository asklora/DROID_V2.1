import numpy as np
import pandas as pd
import scipy.stats as stats
import scipy.special as sc
sc.seterr(all="ignore")

# all days ASCENDING e.g. 0 is the most oldest day

def get_close_vol(multiplier, days_in_year=256):
    returns = multiplier
    log_returns = np.log(returns)
    log_returns_sq = np.square(log_returns)
    var = np.mean(log_returns_sq)
    result = np.sqrt(var * days_in_year)
    return result


def get_total_return(tris):
    total_return = (tris.iloc[-1, :] / tris.iloc[0, :]) - 1
    return total_return


def get_skew(multiplier):
    returns = multiplier
    log_returns = np.log(returns)
    result = stats.skew(log_returns)

    return result


def get_kurt(multiplier):
    returns = multiplier
    log_returns = np.log(returns)
    result = stats.kurtosis(log_returns)
    result = pd.DataFrame(result)
    result.index = multiplier.columns
    return result[0]


def get_rogers_satchell(open_data, high_data, low_data, close_data, days_in_year=256):
    hc_ratio = np.divide(high_data, close_data)
    log_hc_ratio = np.log(hc_ratio.astype(float))
    ho_ratio = np.divide(high_data, open_data)
    log_ho_ratio = np.log(ho_ratio.astype(float))
    lo_ratio = np.divide(low_data, open_data)
    log_lo_ratio = np.log(lo_ratio.astype(float))
    co_ratio = np.divide(close_data, open_data)
    log_co_ratio = np.log(co_ratio.astype(float))
    lc_ratio = np.divide(low_data, close_data)
    log_lc_ratio = np.log(lc_ratio.astype(float))

    input1 = np.multiply(log_hc_ratio, log_ho_ratio)
    input2 = np.multiply(log_lo_ratio, log_lc_ratio)
    sum = np.add(input1, input2)
    rogers_satchell_var = np.mean(sum)
    result = np.sqrt(rogers_satchell_var * days_in_year)

    return result
