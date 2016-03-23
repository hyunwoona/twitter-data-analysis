"""
author: Eric Na (hyunwoo.na@gmail.com)
Reads <score, geo-location> pairs, find the state code from the location, return an array
of average scores for each state.
"""
import sys
from statistics import mean
import json
from geopy.geocoders import Nominatim
from geopy.exc import GeocoderTimedOut

def main():
    """
    Usage: python get_sentiment_by_state.py <input file path>
    input file must be a comma-separated scores, latitudes, and longitudes.
    """
    if len(sys.argv) != 2:
      print('Usage: python get_sentiment_by_state.py <input file path>')
      sys.exit(2)

    input_file_path = sys.argv[1]
    print(get_states_and_scores(input_file_path))

def get_states_and_scores(input_file_path):
    """
    Takes input file path as a parameter, each line of which has comma-separated
    score, latitude, and longitude. Get a JSON array of state-code and average score.
    """
    geolocator = Nominatim()

    state_score_map = {}
    with open(input_file_path, 'r') as input_file:
        for line in input_file:
            values = line.split(",")
            score = float(values[0])
            lat = values[1]
            lng = values[2]
            try:
                # this calculation takes some time
                address = geolocator.reverse(lat + "," + lng, timeout=30)\
                        .address.split(",") # array of components in address (state, zipcode, etc)
                if len(address) > 3 and address[-1] == " United States of America":
                    # is second last item a zipcode or semicolon-separated zipcodes?
                    has_zipcode = address[-2].split(';')[0][1:].isdigit()
                    # third last item, after a space in the front
                    state_name = address[-3][1:] if has_zipcode else address[-2][1:]
                    state_code = get_state_code(state_name)
                    # add score to the list mapped by the state code key
                    if state_code:
                        if state_code not in state_score_map:
                            state_score_map[state_code] = [score, ]
                        else:
                            state_score_map[state_code].append(score)
            except GeocoderTimedOut: #skip if it takes more than 30 seconds
                print("Error: geocode failed on input %s" % (values))

    # construct a list of state-code and average score pair,
    # from a dictionary of state-code and score-list
    states_and_scores = [["State", "Sentiment Score"]] +\
                        [[state, mean(score_list)] for state, score_list in state_score_map.items()]

    # get as json
    return json.dumps(states_and_scores)

def get_state_code(state_name):
    """
    Look up state code from state_name
    """
    state_code_map = {
        'Idaho':'US-ID', 'Indiana':'US-IN', 'Kansas':'US-KS', 'Nevada':'US-NV',
        'North Dakota':'US-ND', 'Massachusetts':'US-MA', 'Montana':'US-MT', 'Maryland':'US-MD',
        'Wisconsin':'US-WI', 'Arkansas':'US-AR', 'Virginia':'US-VA', 'Rhode Island':'US-RI',
        'Delaware':'US-DE', 'Iowa':'US-IA', 'American Samoa':'US-AS', 'Illinois':'US-IL',
        'New Jersey':'US-NJ', 'Vermont':'US-VT', 'Texas':'US-TX', 'Connecticut':'US-CT',
        'Oklahoma':'US-OK', 'Virgin Islands':'US-VI', 'North Carolina':'US-NC',
        'Georgia':'US-GA', 'Puerto Rico':'US-PR', 'Tennessee':'US-TN', 'Florida':'US-FL',
        'Arizona':'US-AZ', 'West Virginia':'US-WV', 'Maine':'US-ME', 'Pennsylvania':'US-PA',
        'Wyoming':'US-WY', 'Alaska':'US-AK', 'Alabama':'US-AL', 'Nebraska':'US-NE',
        'California':'US-CA', 'Colorado':'US-CO', 'Ohio':'US-OH', 'Louisiana':'US-LA',
        'Minnesota':'US-MN', 'New Hampshire':'US-NH', 'Michigan':'US-MI', 'New York':'US-NY',
        'Utah':'US-UT', 'Mississippi':'US-MS', 'South Carolina':'US-SC', 'New Mexico':'US-NM',
        'South Dakota':'US-SD', 'National':'US-NA', 'Kentucky':'US-KY', 'Washington':'US-WA',
        'Missouri':'US-MO', 'District of Columbia':'US-DC', 'Oregon':'US-OR',
    }
    if state_name in state_code_map:
        return state_code_map[state_name]
    else:
        return None

if __name__ == '__main__':
    main()
