from typing import List, Dict


class RideRecord:

    def __init__(self, arr: List[str]):
        self.vendor_id = int(arr[0])
        self.passenger_count = int(arr[1])
        self.trip_distance = float(arr[2])
        self.payment_type = int(arr[3])
        self.total_amount = float(arr[4])

    @classmethod
    def from_dict(cls, d: Dict):
        return cls(arr=[
            d['vendor_id'],
            d['passenger_count'],
            d['trip_distance'],
            d['payment_type'],
            d['total_amount']
        ]
        )

    def __repr__(self):
        return f'{self.__class__.__name__}: {self.__dict__}'


def dict_to_ride_record(obj, ctx):
    if obj is None:
        return None

    return RideRecord.from_dict(obj)


def ride_record_to_dict(ride_record: RideRecord, ctx):
    return ride_record.__dict__

'''
Sure, let's break down this code snippet into simpler terms:

### The `RideRecord` Class
- Imagine `RideRecord` as a detailed record of a taxi ride.
- When you create a new `RideRecord`, you need to provide a list of attributes about the ride, like who the vendor was, how many passengers there were, how far they went, how they paid, and the total cost.
- These attributes are then stored in a way that the `RideRecord` can remember and tell you about later.
- There's also a way to create a `RideRecord` directly from a dictionary (which is like a list but uses names instead of numbers to keep track of each item). This is useful if you're getting your ride information in dictionary form and want to convert it to a `RideRecord`.
- The `__repr__` method is there to give you a quick summary of the ride record whenever you print it or otherwise ask it to represent itself as a string.

### Functions for Data Conversion
- `dict_to_ride_record`: This function helps convert a dictionary into a `RideRecord`. It's handy when you're starting with a dictionary and need a `RideRecord` object to work with in your code.
- `ride_record_to_dict`: This does the reverse by turning a `RideRecord` object back into a dictionary. This might be needed when you want to save the ride information somewhere that requires a dictionary format, like a database or a JSON file.

### Simplified Explanation
- `RideRecord` is like a detailed journal entry for a taxi ride, recording who provided the ride, how many people were in the taxi, the distance traveled, how the passengers paid, and how much they paid.
- You can easily switch between having this information in a dictionary (useful for saving or transferring data) and having it in a structured `RideRecord` object (useful for working with the data in your code).
'''
