from djongo import models


class UniverseHot(models.Model):
    ticker = models.TextField()
    name = models.TextField()
    
    def __str__(self):
        return self.ticker
    
    class Meta:
        db_table = "universe_hot"