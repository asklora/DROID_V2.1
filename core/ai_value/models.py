from django.db import models

class FormulaRatios(models.Model):
    name = models.TextField(blank=True, null=True)
    field_num = models.TextField(blank=True, null=True)
    field_demon = models.TextField(blank=True, null=True)
    calculation = models.TextField(blank=True, null=True)
    fillna = models.BooleanField(blank=True, null=True, default=False)

    class Meta:
        managed = False
        db_table = "ai_value_formula_ratios"

    def __str__(self):
        return self.ticker.ticker


class LgbmEval(models.Model):
    id = models.AutoField(primary_key=True, unique=True)
    index = models.TextField(blank=True, null=True)
    consensus = models.FloatField(blank=True, null=True)
    group_code= models.TextField(blank=True, null=True)
    pred_ind= models.FloatField(blank=True, null=True)
    pred_ind2 = models.FloatField(blank=True, null=True)
    pred_mean = models.FloatField(blank=True, null=True)
    pred_mkt= models.FloatField(blank=True, null=True)
    pred_mkt2 = models.FloatField(blank=True, null=True)
    name_sql= models.TextField(blank=True, null=True)
    class Meta:
        managed = False
        db_table = "ai_value_lgbm_eval"

    def __str__(self):
        return self.ticker.ticker


class LgbmPred(models.Model):
    id = models.AutoField(primary_key=True, unique=True)
    ticker = models.TextField(blank=True, null=True)
    pred = models.FloatField(blank=True, null=True)
    finish_timing= models.DateTimeField(editable=True)
    class Meta:
        managed = False
        db_table = "ai_value_lgbm_pred"

    def __str__(self):
        return self.ticker.ticker


class LgbmPredFinal(models.Model):
    id = models.AutoField(primary_key=True, unique=True)
    ticker= models.TextField(blank=True, null=True)
    testing_period= models.DateTimeField(editable=True),
    pred_mkt= models.FloatField(blank=True, null=True)
    pred_ind= models.FloatField(blank=True, null=True)
    pred_mean = models.FloatField(blank=True, null=True)
    pred_ind2 = models.FloatField(blank=True, null=True)
    pred_mkt2 = models.FloatField(blank=True, null=True)
    name_sql= models.TextField(blank=True, null=True)
    update_time = models.DateTimeField(editable=True)
    class Meta:
        managed = False
        db_table = "ai_value_lgbm_pred_final"

    def __str__(self):
        return self.ticker.ticker


class LgbmPredFinalEps(models.Model):
    id = models.AutoField(primary_key=True, unique=True)
    ticker= models.TextField(blank=True, null=True)
    period_end= models.DateTimeField(editable=True),
    final_pred_eps= models.FloatField(blank=True, null=True)
    consensus_eps = models.FloatField(blank=True, null=True)
    time_type = models.TextField(blank=True, null=True)
    pred_type = models.TextField(blank=True, null=True)
    update_time = models.DateTimeField(editable=True)
    class Meta:
        managed = False
        db_table = "ai_value_lgbm_pred_final_eps"

    def __str__(self):
        return self.ticker.ticker


class LgbmScore(models.Model):
    id = models.AutoField(primary_key=True, unique=True)
    objective = models.TextField(blank=True, null=True)
    y_type= models.TextField(blank=True, null=True)
    qcut_q= models.BigIntegerField(blank=True, null=True)
    backtest_period = models.BigIntegerField(blank=True, null=True)
    max_eval= models.BigIntegerField(blank=True, null=True)
    nthread = models.BigIntegerField(blank=True, null=True)
    name_sql= models.TextField(blank=True, null=True)
    group_code= models.TextField(blank=True, null=True)
    testing_period= models.DateTimeField(editable=True),
    cv_number = models.BigIntegerField(blank=True, null=True)
    train_len = models.BigIntegerField(blank=True, null=True)
    valid_len = models.BigIntegerField(blank=True, null=True)
    finish_timing = models.DateTimeField(editable=True),
    bagging_fraction= models.FloatField(blank=True, null=True)
    bagging_freq= models.BigIntegerField(blank=True, null=True)
    boosting_type = models.TextField(blank=True, null=True)
    feature_fraction= models.FloatField(blank=True, null=True)
    lambda_l1 = models.BigIntegerField(blank=True, null=True)
    lambda_l2 = models.FloatField(blank=True, null=True)
    learning_rate = models.FloatField(blank=True, null=True)
    max_bin = models.BigIntegerField(blank=True, null=True)
    min_data_in_leaf= models.BigIntegerField(blank=True, null=True)
    min_gain_to_split = models.BigIntegerField(blank=True, null=True)
    num_leaves= models.BigIntegerField(blank=True, null=True)
    num_threads = models.BigIntegerField(blank=True, null=True)
    "verbose" = models.BigIntegerField(blank=True, null=True)
    feature_importance= models.TextField(blank=True, null=True)
    mae_train = models.FloatField(blank=True, null=True)
    mae_valid = models.FloatField(blank=True, null=True)
    mse_train = models.FloatField(blank=True, null=True)
    mse_valid = models.FloatField(blank=True, null=True)
    r2_train= models.FloatField(blank=True, null=True)
    r2_valid= models.FloatField(blank=True, null=True)
    test_len = models.BigIntegerField(blank=True, null=True)
    mae_test= models.FloatField(blank=True, null=True)
    mse_test= models.FloatField(blank=True, null=True)
    r2_test = models.FloatField(blank=True, null=True)
    class Meta:
        managed = False
        db_table = "ai_value_lgbm_score"

    def __str__(self):
        return self.ticker.ticker
