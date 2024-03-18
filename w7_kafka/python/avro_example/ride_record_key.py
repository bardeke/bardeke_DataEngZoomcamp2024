from typing import Dict


class RideRecordKey:
    def __init__(self, vendor_id):
        self.vendor_id = vendor_id

    @classmethod
    def from_dict(cls, d: Dict):
        return cls(vendor_id=d['vendor_id'])

    def __repr__(self):
        return f'{self.__class__.__name__}: {self.__dict__}'


def dict_to_ride_record_key(obj, ctx):
    if obj is None:
        return None

    return RideRecordKey.from_dict(obj)


def ride_record_key_to_dict(ride_record_key: RideRecordKey, ctx):
    return ride_record_key.__dict__

'''
The RideRecordKey Class
Think of RideRecordKey as a label for a ride (like a taxi ride) that only cares about who (which vendor) provided the ride.
It's created with a vendor_id (a unique identifier for the vendor).
The class can also turn a dictionary (a simple key-value data structure) into a RideRecordKey object if the dictionary has a vendor_id.
If you ask this class to describe itself (using __repr__), it'll tell you its name and its vendor_id.
Functions for Converting Data
dict_to_ride_record_key: This takes a dictionary and makes a RideRecordKey out of it. If the dictionary is empty, it does nothing.
ride_record_key_to_dict: This does the opposite; it takes a RideRecordKey and turns it back into a dictionary, making it easy to work with in many programming scenarios, especially when saving or sending the data elsewhere.
In Simple Terms
You have a digital label (RideRecordKey) for identifying ride vendors in your system.
You can easily switch between this label and a dictionary, depending on what you need at the moment:
Need to work with the data in your code? Turn the dictionary into a RideRecordKey.
Need to save or send the data somewhere? Turn the RideRecordKey back into a dictionary.

'''