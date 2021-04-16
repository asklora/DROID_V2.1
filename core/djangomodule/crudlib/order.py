from core.orders.models import Order,OrderPosition,PositionPerformance


def sync_order(payload):
    try:
        order = Order.objects.get(order_uid=payload['order_uid'])
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

def sync_position(payload):
    try:
        postion = OrderPosition.objects.get(position_uid=payload['position_uid'])
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

def sync_performance(payload):
    try:
        performance = PositionPerformance.objects.get(performance_uid=payload['performance_uid'])
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