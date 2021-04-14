from core.orders.models import Order,OrderPosition,PositionPerformance


def sync_order(payload):
    try:
        order = Order.objects.get(uid=payload['uid'])
        for attrib, val in payload.items():
            field= order._meta.get_field(attrib)
            if field.one_to_many or field.many_to_many or field.many_to_one or field.one_to_one:
                setattr(order, f'{attrib}_id',val)
            else:
                setattr(order, attrib,val)
        order.save()
    except Order.DoesNotExist:
        attribs_modifier = {}
        for attrib, val in payload.items():
            field= Order._meta.get_field(attrib)
            if field.one_to_many or field.many_to_many or field.many_to_one or field.one_to_one:
                attribs_modifier[f'{attrib}_id'] = val
            else:
                attribs_modifier[attrib]=val
        order = Order.objects.create(**attribs_modifier)
    
    print(order)

def sync_postion(payload):
    try:
        postion = OrderPosition.objects.get(uid=payload['uid'])
        for attrib, val in payload.items():
            field= postion._meta.get_field(attrib)
            if field.one_to_many or field.many_to_many or field.many_to_one or field.one_to_one:
                setattr(postion, f'{attrib}_id',val)
            else:
                setattr(postion, attrib,val)
        postion.save()
    except OrderPosition.DoesNotExist:
        attribs_modifier = {}
        for attrib, val in payload.items():
            field= OrderPosition._meta.get_field(attrib)
            if field.one_to_many or field.many_to_many or field.many_to_one or field.one_to_one:
                attribs_modifier[f'{attrib}_id'] = val
            else:
                attribs_modifier[attrib]=val
        postion = OrderPosition.objects.create(**attribs_modifier)
    
    print(postion)

def sync_perfromance(payload):
    try:
        performance = PositionPerformance.objects.get(uid=payload['uid'])
        for attrib, val in payload.items():
            field= performance._meta.get_field(attrib)
            if field.one_to_many or field.many_to_many or field.many_to_one or field.one_to_one:
                setattr(performance, f'{attrib}_id',val)
            else:
                setattr(performance, attrib,val)
        performance.save()
    except PositionPerformance.DoesNotExist:
        attribs_modifier = {}
        for attrib, val in payload.items():
            field= PositionPerformance._meta.get_field(attrib)
            if field.one_to_many or field.many_to_many or field.many_to_one or field.one_to_one:
                attribs_modifier[f'{attrib}_id'] = val
            else:
                attribs_modifier[attrib]=val
        performance = PositionPerformance.objects.create(**attribs_modifier)
    
    print(performance)