from django.db import models
from core.djangomodule.models import BaseTimeStampModel
from core.universe.models import Universe
# Create your models here.

class Transaction(BaseTimeStampModel): 
	side = models.CharField(max_length=5) # buy or sell
	ticker_symbol = models.ForeignKey(Universe,on_delete=models.CASCADE)
	asset_id = models.CharField(max_length=255)
	client_order_id = models.CharField(max_length=255)
	qty = models.IntegerField()
	price = models.BigIntegerField()
	status = models.CharField(max_length=20) # Pending / Canceled / Approved
	subbmitted_at = models.DateTimeField()
	filled_at = models.DateTimeField(null=True, blank=True, default=None)

	class Meta:
		managed = True
		db_table = "transaction"

# "asset_class": "us_equity",
# "canceled_at": null,
# "expired_at": null,
# "extended_hours": true,
# "failed_at": null,
# "filled_avg_price": null,
# "filled_qty": "0",
# "hwm": null,
# "legs": null,
# "notional": null,
# "order_class": "simple",
# "order_type": "limit",
# "replaced_at": null,
# "replaced_by": null,
# "replaces": null,
# "stop_price": null,
# "time_in_force": "day",
# "trail_percent": null,
# "trail_price": null,
# "type": "limit",

# "id": "5777a88c-b3e1-4801-bbfc-625c4528ff6c",
# "asset_id": "8ccae427-5dd0-45b3-b5fe-7ba5e422c766",
# "client_order_id": "dc10c718fe09487f9b217cef9d4a1c32",
# "qty": "1",
# "limit_price": "500",
# "symbol": "TSLA",
# "filled_at": null,
# "status": "accepted",
# "side": "buy",
# "created_at": "2021-03-16T05:07:34.397413Z",
# "submitted_at": "2021-03-16T05:07:34.3928Z",
# "updated_at": "2021-03-16T05:07:34.397413Z"