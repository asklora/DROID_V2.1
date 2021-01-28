import base64
import os
from django.utils.deconstruct import deconstructible

@deconstructible
class UploadTo:
    def __init__(self, name):
        self.name = name

    def __call__(self, instance, filename):
        ext = filename.split('.')[-1]
        filename = f"{instance.app.creator.first_name.upper()}_{instance.app.creator.last_name.upper()}_{self.name}_{instance.app.uid}.{ext}"
        return '{0}/{1}'.format(instance.app.app_type.name, filename)



def generate_id(digit):
    r_id = base64.b64encode(os.urandom(digit)).decode('ascii')
    r_id = r_id.replace(
        '/', '').replace('_', '').replace('+', '').strip()
    return r_id