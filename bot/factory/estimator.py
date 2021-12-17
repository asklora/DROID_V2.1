import datetime
import gc
from multiprocessing import cpu_count
from general.sql_query import get_bot_vol_surface_data
from global_vars import (
    large_hedge,
    small_hedge,
    buy_UCDC_prem,
    sell_UCDC_prem,
    buy_UNO_prem,
    sell_UNO_prem,
    max_vol,
    min_vol,
    default_vol,
)

import numpy as np
import scipy.special as sc
import scipy.stats as si
from bot.factory.utilities import BotUtilities
from general.data_process import NoneToZero
from scipy.optimize import newton
from sqlalchemy import create_engine
from sqlalchemy.sql import text

from .AbstractBase import AbstractCalculator
from .validator import BotCreateProps
from .botproperties import EstimatorUnoResult, EstimatorUcdcResult

sc.seterr(all="ignore")


class BlackScholes(AbstractCalculator, BotUtilities):
    def calculate(self):
        pass

    def BS_call(self, S, K, T, r, q, sigma):
        # S: spot price
        # K: strike price
        # T: time to maturity
        # r: interest rate
        # q: rate of continuous dividend paying asset
        # sigma: volatility of underlying asset

        d1 = (np.log(S / K) + (r - q + 0.5 * sigma ** 2) * T) / (
            sigma * np.sqrt(T)
        )
        d2 = d1 - (sigma * np.sqrt(T))

        call = (S * np.exp(-q * T) * si.norm.cdf(d1, 0.0, 1.0)) - (
            K * np.exp(-r * T) * si.norm.cdf(d2, 0.0, 1.0)
        )

        return call

    def BS_put(self, S, K, T, r, q, sigma):
        # S: spot price
        # K: strike price
        # T: time to maturity
        # r: interest rate
        # q: rate of continuous dividend paying asset
        # sigma: volatility of underlying asset

        d1 = (np.log(S / K) + (r - q + 0.5 * sigma ** 2) * T) / (
            sigma * np.sqrt(T)
        )
        d2 = d1 - (sigma * np.sqrt(T))

        put = (K * np.exp(-r * T) * si.norm.cdf(-d2, 0.0, 1.0)) - (
            S * np.exp(-q * T) * si.norm.cdf(-d1, 0.0, 1.0)
        )

        return put

    def delta_to_strikes(self, S, d, T, r, q, sigma):
        # S: spot price
        # d: delta
        # T: time to maturity
        # r: interest rate
        # q: rate of continuous dividend paying asset
        # sigma: volatility of underlying asset

        d1 = (
            si.norm.ppf(d) * (sigma * np.sqrt(T))
            - (r - q + 0.5 * sigma ** 2) * T
        )
        strike = S / np.exp(d1)
        # si.norm.ppf(5)
        return strike

    def bs_call_delta(self, S, K, T, r, q, sigma):
        # S: spot price
        # d: delta
        # T: time to maturity
        # r: interest rate
        # q: rate of continuous dividend paying asset
        # sigma: volatility of underlying asset

        d1 = (np.log(S / K) + (r - q + 0.5 * sigma ** 2) * T) / (
            sigma * np.sqrt(T)
        )
        delta = si.norm.cdf(d1)

        return delta

    def delta_vol(
        self,
        delta,
        t,
        atmv_0,
        atmv_1Y,
        atmv_inf,
        term_alpha,
        skew_1m,
        skew_inf,
        curv_1m,
        curv_inf,
    ):
        # t = days to maturity / 365

        # term structure
        if t > 0.05:
            st_vol = (
                (atmv_0 - atmv_inf)
                * (1 - np.exp(-term_alpha * t))
                / (term_alpha * t)
            )
            # print(st_vol)
            lt_vol = np.exp(-term_alpha * t) * (atmv_1Y - atmv_inf)
            atm_vol = atmv_inf + st_vol - lt_vol
        else:  # for really small t use a hack
            atmv_0 * (1 - t / 0.08) + (atmv_1Y * t / 0.08)
            atm_vol = atmv_0 * (1 - t / 0.08) + atmv_1Y * (
                t / 0.08
            )  # 0.05 / 0.08 so use some atmv_1Y

        # find the skew + curvature
        t_ratio = np.sqrt(1 / (t * 12))
        if t_ratio > 1.5:
            t_ratio = (
                1.5  # limit short term skew to 1.5 X skew_1m - 0.5 X skew_inf
            )
        skew = skew_1m * t_ratio + skew_inf * (1 - t_ratio)
        if t_ratio > 1:
            t_ratio = 1  # limit to curv => curv_1m
        curv = curv_1m * t_ratio + curv_inf * (1 - t_ratio)

        vol = atm_vol * (
            1
            + (skew * (delta * 100 - 50) / 1000)
            + (curv * ((delta * 100 - 50) ** 2) / 2000)
        )
        if vol < 0:
            vol = 0.01  # don't return NEGATIVE vols

        return vol

    def find_vol(
        self,
        moneyness,
        expiry_days_as_fraction,
        atmv_0,
        atmv_1Y,
        atmv_inf,
        term_alpha,
        skew_1m,
        skew_inf,
        curv_1m,
        curv_inf,
        r,
        q,
    ):
        # input MONEYNESS - not BS forwards OR price strikes
        moneyness = moneyness * 100  # USE 100 BASE!!!

        def strike_delta(delta):

            vol = self.delta_vol(
                delta,
                expiry_days_as_fraction,
                atmv_0,
                atmv_1Y,
                atmv_inf,
                term_alpha,
                skew_1m,
                skew_inf,
                curv_1m,
                curv_inf,
            )
            # print(vol)
            K = self.delta_to_strikes(
                100, delta, expiry_days_as_fraction, r, q, vol
            )
            # print(K)
            return K - moneyness

        # find a vol for the strike
        try:
            final_delta = newton(strike_delta, 0.5, maxiter=1000)
            # print(final_delta)
        except RuntimeError as E:
            # try starting a seed at 0.9 or 0.1 delta
            try:
                final_delta = newton(strike_delta, x0=0.9, maxiter=1000)
            except RuntimeError as E:
                try:
                    final_delta = newton(strike_delta, x0=0.1, maxiter=1000)
                except RuntimeError as E:
                    # if still not converging, just take the zero or one delta vol
                    if (
                        moneyness * ((expiry_days_as_fraction * (r - q)) + 1)
                        > 100
                    ):
                        # if BS forward is greater than 100 - means higher strike
                        final_delta = 0.01
                    else:
                        final_delta = 0.99

        final_delta = min(max(final_delta, 0.01), 0.99)

        final_vol = self.delta_vol(
            final_delta,
            expiry_days_as_fraction,
            atmv_0,
            atmv_1Y,
            atmv_inf,
            term_alpha,
            skew_1m,
            skew_inf,
            curv_1m,
            curv_inf,
        )

        final_vol = max(final_vol, 0.1)

        return final_vol

    def get_vol(
        self,
        ticker: str,
        trading_day: datetime,
        t: int,
        r: float,
        q: float,
        time_to_exp: float,
    ):
        status, obj = self.get_vol_by_date(ticker, trading_day)
        # print(status, obj)

        if status:
            v0 = self.find_vol(
                1,
                t / 365,
                obj["atm_volatility_spot"],
                obj["atm_volatility_one_year"],
                obj["atm_volatility_infinity"],
                12,
                obj["slope"],
                obj["slope_inf"],
                obj["deriv"],
                obj["deriv_inf"],
                r,
                q,
            )
            v0 = np.nan_to_num(v0, nan=0)
            v0 = max(min(v0, max_vol), min_vol)

            # Code when we use business days
            # month = int(round((time_exp * 256), 0)) / 22
            month = int(round((time_to_exp * 365), 0)) / 30
            vol = v0 * (month / 12) ** 0.5

        else:
            vol = default_vol
        return float(NoneToZero(vol))

    def get_vol_by_date(self, ticker, trading_day):
        return get_bot_vol_surface_data(ticker, str(trading_day))

    def get_v1_v2(
        self,
        ticker: str,
        price: float,
        trading_day: datetime,
        t: int,
        r: float = 0,
        q: float = 0,
        strike: float = 0,
        barrier: float = 0,
    ):
        status, obj = self.get_vol_by_date(ticker, trading_day)
        if status:
            v1 = self.find_vol(
                (strike / price),
                t / 365,
                obj["atm_volatility_spot"],
                obj["atm_volatility_one_year"],
                obj["atm_volatility_infinity"],
                12,
                obj["slope"],
                obj["slope_inf"],
                obj["deriv"],
                obj["deriv_inf"],
                r,
                q,
            )
            v1 = np.nan_to_num(v1, nan=0)
            v2 = self.find_vol(
                NoneToZero(barrier / price),
                t / 365,
                obj["atm_volatility_spot"],
                obj["atm_volatility_one_year"],
                obj["atm_volatility_infinity"],
                12,
                obj["slope"],
                obj["slope_inf"],
                obj["deriv"],
                obj["deriv_inf"],
                r,
                q,
            )
            v2 = np.nan_to_num(v2, nan=0)
        else:
            v1 = 0
            v2 = 0
        return float(NoneToZero(v1)), float(NoneToZero(v2))

    def convert_ORATS_vol(self, iv30, iv60, iv90, M3ATM, M4ATM):
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
        spot_vol = (
            one_mon_v - inf_vol + 0.38 * (one_year_v - inf_vol)
        ) / 0.62 + inf_vol

        return spot_vol, one_year_v, inf_vol

    def Up_Out_Call(self, S, K, Bar, Reb, T, r, q, v1, v2):
        #   S- Spot, K -Strike, Bar - Barrier
        #   v1 is strike vol, #v2 is BARRIER vol - price with Reb = Bar - K
        v3 = (v1 + v2) / 2
        phi = 1
        eta = -1
        mu1 = (r - q - (v1 ** 2) / 2) / (v1 ** 2)
        mu2 = (r - q - (v2 ** 2) / 2) / (v2 ** 2)
        mu3 = (r - q - (v3 ** 2) / 2) / (v3 ** 2)
        lmbda = np.sqrt(
            (mu2 ** 2) + (2 * r) / (v2 ** 2)
        )  # only used for F (one Touch Barrier)
        z = np.log(Bar / S) / (v2 * np.sqrt(T)) + (
            lmbda * v2 * np.sqrt(T)
        )  # only used for F (one Touch Barrier)

        x1 = np.log(S / K) / (v1 * np.sqrt(T)) + (
            (1 + mu1) * v1 * np.sqrt(T)
        )  # call option with strike
        x2 = np.log(S / Bar) / (v2 * np.sqrt(T)) + (
            (1 + mu2) * v2 * np.sqrt(T)
        )  # barrier
        y1 = np.log((Bar ** 2) / (S * K)) / (v3 * np.sqrt(T)) + (
            (1 + mu3) * v3 * np.sqrt(T)
        )  # both
        y2 = np.log(Bar / S) / (v2 * np.sqrt(T)) + (
            (1 + mu2) * v2 * np.sqrt(T)
        )  # barrier

        # A:v1, B:v2, C:v3, D:v2, F:v2
        A = phi * S * np.exp(-q * T) * si.norm.cdf(phi * x1) - phi * K * np.exp(
            -r * T
        ) * si.norm.cdf(
            phi * x1 - phi * v1 * np.sqrt(T)
        )  # call option with strike
        B = phi * S * np.exp(-q * T) * si.norm.cdf(phi * x2) - phi * K * np.exp(
            -r * T
        ) * si.norm.cdf(
            phi * x2 - phi * v2 * np.sqrt(T)
        )  # barrier
        C = phi * S * np.exp(-q * T) * (Bar / S) ** (2 * mu3 + 2) * si.norm.cdf(
            eta * y1
        ) - phi * K * np.exp(-r * T) * (Bar / S) ** (2 * mu3) * si.norm.cdf(
            eta * y1 - eta * v3 * np.sqrt(T)
        )
        D = phi * S * np.exp(-q * T) * (Bar / S) ** (2 * mu2 + 2) * si.norm.cdf(
            eta * y2
        ) - phi * K * np.exp(-r * T) * (Bar / S) ** (2 * mu2) * si.norm.cdf(
            eta * y2 - eta * v2 * np.sqrt(T)
        )
        F = Reb * (
            ((Bar / S) ** (mu2 + lmbda)) * si.norm.cdf(eta * z)
            + ((Bar / S) ** (mu2 - lmbda))
            * si.norm.cdf(eta * z - 2 * eta * lmbda * v2 * np.sqrt(T))
        )

        UnOC = A - B + C - D + F

        return UnOC

    def Rev_Conv(self, S, K1, K2, T, r, q, v1, v2):
        p1 = self.BS_put(S, K1, T, r, q, v1)
        p2 = self.BS_put(S, K2, T, r, q, v2)

        rev_conv = p2 - p1

        return rev_conv

    def One_T_Bar(self, S, K, Bar, Reb, T, r, q, v):
        # ONLY use for pricing the one touch rebate
        eta = -1
        mu = (r - q - (v ** 2) / 2) / (v ** 2)
        lmbda = np.sqrt((mu ** 2) + (2 * r) / (v ** 2))
        z = np.log(Bar / S) / (v * np.sqrt(T)) + (lmbda * v * np.sqrt(T))

        F = Reb * (
            ((Bar / S) ** (mu + lmbda)) * si.norm.cdf(eta * z)
            + ((Bar / S) ** (mu - lmbda))
            * si.norm.cdf(eta * z - 2 * eta * lmbda * v * np.sqrt(T))
        )

        return F

    def deltaUnOC(self, S, K, Bar, Reb, T, r, q, v1, v2):
        # delta of up and out KO call
        p1 = self.Up_Out_Call(S * 1.01, K, Bar, Reb, T, r, q, v1, v2)
        p2 = self.Up_Out_Call(S * 0.99, K, Bar, Reb, T, r, q, v1, v2)

        delta = (p1 - p2) / (S * 0.02)

        return delta

    def deltaRC(self, S, K1, K2, T, r, q, v1, v2):
        # delta of up and out KO call
        p1 = self.Rev_Conv(S * 1.01, K1, K2, T, r, q, v1, v2)
        p2 = self.Rev_Conv(S * 0.99, K1, K2, T, r, q, v1, v2)

        delta = (p1 - p2) / (S * 0.02)

        return delta

    def get_interest_rate(self, index_code, spot_date, expiry_date):
        # PLEASE convert to GLOBAL VARS and VECTORIZE!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
        # psedo function!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
        jdbc_url = "postgres://postgres:askLORA20$@droid-test.cgqhw7rofrpo.ap-northeast-2.rds.amazonaws.com:5432/postgres"
        db_engine = create_engine(
            jdbc_url,
            pool_size=cpu_count(),
            max_overflow=-1,
            isolation_level="AUTOCOMMIT",
        )
        # get CURRENCY CODE given index ~ ticker → index → indices table → currency_code → interest_rate table
        get_curr_sql = text(
            "select currency_code from currency WHERE index =:index_code"
        )
        with db_engine.connect() as conn:
            currency_code = conn.execute(
                get_curr_sql, index_code=index_code
            ).fetchall()
        del conn
        gc.collect()
        x = currency_code[0][0]
        # get rates and days_to_maturity arrays for the currency
        get_rates_sql = text(
            "select rate, days_to_maturity from interest_rate WHERE currency = :currency_code ORDER BY days_to_maturity ASC"
        )

        # get rates from db for that currency
        with db_engine.connect() as conn:
            rates_and_dm = conn.execute(
                get_rates_sql, currency_code=currency_code[0][0]
            ).fetchall()
        del conn
        gc.collect()
        # dispose engine
        db_engine.dispose()

        # ACTUAL calendar days from spot to expiry
        # date_1 = datetime.strptime(spot_date,'%Y-%m-%d')
        # date_2 = datetime.strptime(expiry_date,'%Y-%m-%d')
        #
        # t_delta = date_2 - date_1
        t_delta = expiry_date - spot_date
        days_to_expiry = int(t_delta.days)
        rate_2 = 0
        dtm_2 = 0
        # find the rate that is between TWO maturities
        for this_rate in rates_and_dm:
            # get rate
            rate_1 = rate_2
            dtm_1 = dtm_2
            rate_2 = this_rate["rate"] / 100
            dtm_2 = this_rate["days_to_maturity"]
            if dtm_2 >= days_to_expiry:
                break

        # the rate calculation for the spot/expiry date
        if days_to_expiry >= dtm_2:
            rate = rate_2
        elif days_to_expiry <= dtm_1:
            rate = rate_1
        else:
            # weighted average - WEIGHT THE reverse days to mat over total dates
            rate = rate_1 * (dtm_2 - days_to_expiry) / (
                dtm_2 - dtm_1
            ) + rate_2 * (days_to_expiry - dtm_1) / (dtm_2 - dtm_1)

        # return rate
        return rate


class UnoCreateEstimator(BlackScholes):
    @property
    def _uno_strike_itm(self):
        return self.validated_data.price * (1 + self.vol * 0.5)

    @property
    def _uno_strike_otm(self):
        return self.validated_data.price * (1 + self.vol * 0.5)

    @property
    def _uno_barier_itm(self):
        return self.validated_data.price * (1 + self.vol * 2)

    @property
    def _uno_barier_otm(self):
        return self.validated_data.price * (1 + self.vol * 1.5)

    @property
    def _rebate(self):
        return abs(self._get_strike - self._get_barrier)

    @property
    def _get_strike(self):
        return getattr(
            self,
            f"_uno_strike_{self.validated_data.bot.bot_option_type.lower()}",
        )

    @property
    def _get_barrier(self):
        return getattr(
            self,
            f"_uno_barier_{self.validated_data.bot.bot_option_type.lower()}",
        )

    def calculate(self, validated_data: BotCreateProps):
        self.validated_data = validated_data
        t, r, q = self.get_trq(
            validated_data.expiry,
            validated_data.spot_date,
            validated_data.ticker,
            validated_data.currency,
        )
        self.vol = self.get_vol(
            validated_data.ticker,
            validated_data.spot_date,
            t,
            r,
            q,
            validated_data.time_to_exp,
        )

        v1, v2 = self.get_v1_v2(
            validated_data.ticker,
            validated_data.price,
            validated_data.spot_date,
            t,
            r,
            q,
            self._get_strike,
            self._get_barrier,
        )
        delta = self.deltaUnOC(
            validated_data.price,
            self._get_strike,
            self._get_barrier,
            self._rebate,
            t / 365,
            r,
            q,
            v1,
            v2,
        )
        delta = np.nan_to_num(delta, nan=0)
        option_price = self.Up_Out_Call(
            validated_data.price,
            self._get_strike,
            self._get_barrier,
            self._rebate,
            t / 365,
            r,
            q,
            v1,
            v2,
        )
        option_price = np.nan_to_num(option_price, nan=0)

        result = EstimatorUnoResult(
            t=t,
            r=r,
            q=q,
            vol=self.vol,
            v1=v1,
            v2=v2,
            barrier=self._get_barrier,
            rebate=self._rebate,
            strike=self._get_strike,
            delta=delta,
            option_price=option_price,
        )
        return result


class UcdcCreateEstimator(BlackScholes):
    @property
    def _get_strike(self):
        return self.validated_data.price

    @property
    def _get_strike_2(self):
        return self.validated_data.price * (1 - self.vol * 1.5)

    def calculate(self, validated_data):
        self.validated_data = validated_data
        t, r, q = self.get_trq(
            validated_data.expiry,
            validated_data.spot_date,
            validated_data.ticker,
            validated_data.currency,
        )
        self.vol = self.get_vol(
            validated_data.ticker,
            validated_data.spot_date,
            t,
            r,
            q,
            validated_data.time_to_exp,
        )

        v1, v2 = self.get_v1_v2(
            validated_data.ticker,
            validated_data.price,
            validated_data.spot_date,
            t,
            r,
            q,
            self._get_strike,
            self._get_strike_2,
        )
        delta = self.deltaRC(
            validated_data.price,
            self._get_strike,
            self._get_strike_2,
            t / 365,
            r,
            q,
            v1,
            v2,
        )
        delta = np.nan_to_num(delta, nan=0)
        option_price = self.Rev_Conv(
            validated_data.price,
            self._get_strike,
            self._get_strike_2,
            t / 365,
            r,
            q,
            v1,
            v2,
        )
        option_price = np.nan_to_num(option_price, nan=0)

        return EstimatorUcdcResult(
            t=t,
            r=r,
            q=q,
            vol=self.vol,
            v1=v1,
            v2=v2,
            strike_2=self._get_strike_2,
            strike=self._get_strike,
            delta=delta,
            option_price=option_price,
        )
