from django.db import models
from core.djangomodule.models import BaseTimeStampModel
from core.universe.models import Universe
from core.user.models import User
from django.db import IntegrityError
import uuid
# Create your models here.

class OrderPositon(BaseTimeStampModel):
	uid = models.UUIDField(primary_key=True, editable=False)
	user = models.ForeignKey(User, on_delete=models.CASCADE, related_name='user_order_position')
	investment_amount = models.FloatField()
	market_value = models.FloatField()
	pnl_percentage = models.FloatField()
	pnl_value = models.FloatField()
	use_bot = models.BooleanField(default=False)
	bot_id = models.CharField(max_length=255,null=True,blank=True) #user = stock
	bot = models.JSONField(null=True, blank=True)
	symbol=models.ForeignKey(Universe, on_delete=models.SET_NULL,null=True, blank=True, related_name='portfolio_symbol')
	entry_price = models.FloatField()
	avg_price = models.FloatField(null=True, blank=True)
	shares = models.FloatField()
	is_live = models.BooleanField(default=True)
	max_loss_pct = models.FloatField(null=True, blank=True)
	max_loss_price = models.FloatField(null=True, blank=True)
	max_loss_amount = models.FloatField(null=True, blank=True)
	target_profit_pct = models.FloatField(null=True, blank=True)
	target_profit_price = models.FloatField(null=True, blank=True)
	target_profit_amount = models.FloatField(null=True, blank=True)
	bot_cash_balance = models.FloatField(null=True, blank=True)
	event = models.CharField(max_length=75, null=True, blank=True)
	event_date = models.DateField(null=True, blank=True)
	final_price = models.FloatField(null=True, blank=True)
	final_return = models.FloatField(null=True, blank=True)
	final_pnl_amount = models.FloatField(null=True, blank=True)
	commision_fee_buy = models.FloatField(null=True, blank=True, default=0)
	commision_fee_sell = models.FloatField(null=True, blank=True, default=0)
	

	class Meta:
		managed = True
		db_table = "order_position"

	def save(self, *args, **kwargs):
		if not self.uid:
			self.uid = uuid.uuid4().hex
			# using your function as above or anything else
			success = False
			failures = 0
			while not success:
				try:
					super(OrderPositon, self).save(*args, **kwargs)
				except IntegrityError:
					failures += 1
					if failures > 5:  # or some other arbitrary cutoff point at which things are clearly wrong
						raise KeyError
					else:
						# looks like a collision, try another random value
						self.uid = uuid.uuid4().hex
				else:
					success = True
		else:
			super().save(*args, **kwargs)




class OrderHistory(BaseTimeStampModel):
	position = models.ForeignKey(
	    OrderPositon, on_delete=models.CASCADE, related_name='order_position')
	entry_price = models.FloatField(null=True, blank=True)
	current_pnl_ret = models.FloatField(null=True, blank=True)
	current_pnl_amt = models.FloatField(null=True, blank=True)
	current_bot_cash_balance = models.FloatField(null=True, blank=True)
	share_num = models.FloatField(null=True, blank=True)
	current_investment_amount = models.FloatField(null=True, blank=True)
	last_hedge_delta = models.FloatField(null=True, blank=True)
	side = models.CharField(max_length=5) # sell,hold,buy
	filled_avg_price = models.FloatField()
	status = models.CharField(max_length=20) # pending, filled, canceled
	filled_at = models.DateTimeField(null=True,blank=True)
	canceled_at = models.DateTimeField(null=True,blank=True)
	amount = models.FloatField()
	option_price =models.FloatField(blank=True, null=True)
	strike = models.FloatField(blank=True, null=True)
	barrier = models.FloatField(blank=True, null=True)
	r = models.FloatField(blank=True, null=True)
	q = models.FloatField(blank=True, null=True)
	v1 = models.FloatField(blank=True, null=True)
	v2 = models.FloatField(blank=True, null=True)
	delta = models.FloatField(blank=True, null=True)
	strike_2 = models.FloatField(blank=True, null=True)
	
	order_summary = models.JSONField( null=True, blank=True) # order response from third party

	class Meta:
		managed = True
		db_table = "order_transaction_history"
		
	def __str__(self):
		return self.side
