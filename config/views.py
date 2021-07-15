from django.views.generic import TemplateView


class IndexWs(TemplateView):
    
    template_name = 'index.html'