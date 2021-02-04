from django.forms import forms,ModelForm
from .models import UniverseConsolidated 



class AddTicker(ModelForm):

    
    class Meta:
        model = UniverseConsolidated
        fields = ('origin_ticker','source_id','use_isin','use_manual')