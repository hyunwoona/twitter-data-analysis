from geopy.geocoders import Nominatim
from geopy.exc import GeocoderTimedOut
from statistics import mean
import json


def main():
    geolocator = Nominatim()

    state_score_map = {}
    with open('sentiment.csv', 'r') as f:
        for line in f:
            values = line.split(",")
            score = float(values[0])
            lat = values[1]
            lng = values[2]
            try:
                address = geolocator.reverse(lat + "," + lng, timeout=15).address.split(",")
                if address[-1] == " United States of America":
                    state_name = address[-3][1:] #third last item, after a space in the front
                    state_code = get_state_code(state_name)
                    
                    if score != 0.0:
                        if state_code not in state_score_map:
                            state_score_map[state_code] = [score, ]
                        else:
                            state_score_map[state_code].append(score)
            except GeocoderTimedOut as e:
                print("Error: geocode failed on input %s with message %s"%(values, e.message))
        
    states_and_scores = [["State", "Sentiment Score"]] + [[state, mean(score_list)] for state, score_list in state_score_map.items()]

    states_and_scores_in_json = json.dumps(states_and_scores)
    print(states_and_scores_in_json)

def get_state_code(state_name):
    state_code_map = {'Idaho':'US-ID', 'Indiana':'US-IN', 'Kansas':'US-KS', 'Nevada':'US-NV', 'North Dakota':'US-ND', 'Massachusetts':'US-MA', 'Montana':'US-MT', 'Maryland':'US-MD', 'Wisconsin':'US-WI', 'Arkansas':'US-AR', 'Virginia':'US-VA', 'Rhode Island':'US-RI', 'Delaware':'US-DE', 'Iowa':'US-IA', 'American Samoa':'US-AS', 'Northern Mariana Islands':'US-MP', 'Illinois':'US-IL', 'New Jersey':'US-NJ', 'Vermont':'US-VT', 'Texas':'US-TX', 'Connecticut':'US-CT', 'Oklahoma':'US-OK', 'Virgin Islands':'US-VI', 'North Carolina':'US-NC', 'Georgia':'US-GA', 'Puerto Rico':'US-PR', 'Tennessee':'US-TN', 'Florida':'US-FL', 'Arizona':'US-AZ', 'West Virginia':'US-WV', 'Maine':'US-ME', 'Pennsylvania':'US-PA', 'Wyoming':'US-WY', 'Alaska':'US-AK', 'Alabama':'US-AL', 'Nebraska':'US-NE', 'California':'US-CA', 'Colorado':'US-CO', 'Ohio':'US-OH', 'Louisiana':'US-LA', 'Minnesota':'US-MN', 'New Hampshire':'US-NH', 'Michigan':'US-MI', 'New York':'US-NY', 'Utah':'US-UT', 'Mississippi':'US-MS', 'Hawaii':'US-HI', 'South Carolina':'US-SC', 'South Dakota':'US-SD', 'National':'US-NA', 'Kentucky':'US-KY', 'Washington':'US-WA', 'Missouri':'US-MO', 'District of Columbia':'US-DC', 'Oregon':'US-OR', 'Guam':'US-GU', 'New Mexico':'US-NM',}
    if state_name in state_code_map:
        return state_code_map[state_name]
    else:
        return state_name

if __name__=='__main__':
    main()
