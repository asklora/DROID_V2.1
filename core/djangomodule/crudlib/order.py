from core.orders.models import Order,OrderPosition,PositionPerformance


def sync_order(payload):
    try:
        order = Order.objects.get(uid=payload['uid'])
        for attrib, val in payload.items():
            setattr(order, attrib,val)
        order.save()
    except Order.DoesNotExist:
        order = Order.objects.create(**payload)
    
    print(order)

def sync_postion(payload):
    try:
        position = OrderPosition.objects.get(uid=payload['uid'])
        for attrib, val in payload.items():
            setattr(position, attrib,val)
        position.save()
    except OrderPosition.DoesNotExist:
        position = OrderPosition.objects.create(**payload)
    
    print(position)

def sync_perfromance(payload):
    try:
        performance = PositionPerformance.objects.get(uid=payload['uid'])
        for attrib, val in payload.items():
            setattr(performance, attrib,val)
        performance.save()
    except PositionPerformance.DoesNotExist:
        performance = PositionPerformance.objects.create(**payload)
    
    print(performance)