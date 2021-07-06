from core.orders.models import Order,OrderPosition,PositionPerformance

# master, LatestPrice, [{data}] --> df_data.to_dict('records')
def save_latest_price(app='master', model='LatestPrice', data):
    from django.apps import apps
    Model = apps.get_model(app, model)
    pk = Model._meta.pk.name
    if isinstance(data, list):
        if isinstance(data[0], dict):
            pass
        else:
            raise Exception('data should be dataframe or list of dict')
    elif isinstance(data, pd.DataFrame):
        data = data.to_dict("records")
    else:
        raise Exception('data should be dataframe or dict')
    key_set = [key for key in data[0].keys()]
    list_obj = []
    for item in data:
        if pk in key_set:
            key_set.remove(pk)
        try:
            key = {pk: item[pk]}
            obj = Model.objects.get(**key)
        except Model.DoesNotExist:
            raise Exception(f'models {item[pk]} does not exist')
        except KeyError:
            raise Exception('no primary key in dict')
        for attr, val in item.items():
            field = obj._meta.get_field(attr)
            if field.one_to_many or field.many_to_many or field.many_to_one or field.one_to_one:
                attr = f'{attr}_id'
            setattr(obj, attr, val)
        list_obj.append(obj)
    Model.objects.bulk_update(list_obj, key_set, batch_size=500)

# def sync_order(payload):
#     try:
#         order = Order.objects.get(order_uid=payload['order_uid'])
#         for attrib, val in payload.items():
#             field= order._meta.get_field(attrib)
#             if field.one_to_many or field.many_to_many or field.many_to_one or field.one_to_one:
#                 setattr(order, f'{attrib}_id',val)
#             else:
#                 setattr(order, attrib,val)
#         order.save()
#     except Order.DoesNotExist:
#         attribs_modifier = {}
#         for attrib, val in payload.items():
#             field= Order._meta.get_field(attrib)
#             if field.one_to_many or field.many_to_many or field.many_to_one or field.one_to_one:
#                 attribs_modifier[f'{attrib}_id'] = val
#             else:
#                 attribs_modifier[attrib]=val
#         order = Order.objects.create(**attribs_modifier)
    
#     print(order)