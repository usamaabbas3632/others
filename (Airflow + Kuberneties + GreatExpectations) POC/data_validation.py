import sys
import json
import great_expectations as ge


def validate_data(path):
	df = ge.read_csv(path)

	df.expect_column_mean_to_be_between("Age", 20,40)
	df.expect_column_values_to_be_between("Age", 0,80)
	df.expect_column_values_to_match_regex('Name', '[A-Z][a-z]+(?: \([A-Z][a-z]+\))?, ', mostly=.95)
	df.expect_column_values_to_be_in_set('Sex', ['male', 'female'])
	df.expect_column_values_to_be_in_set('Survived', [1, 0])
	df.expect_column_values_to_be_in_set('PClass', ['1st', '2nd', '3rd'])

	return df.validate().to_json_dict()


if __name__ == "__main__":
	path = sys.argv[1]
	#path = "./titanic.csv"

	res = validate_data(path)
	res = json.dumps(res, indent=2)
	print(res)
