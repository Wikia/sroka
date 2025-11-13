

def input_check(input_to_check, expected_types):
    for expected_type in expected_types:
        if type(input_to_check) == expected_type:
            if expected_type == str and len(input_to_check) == 0:
                print('Function input must be a nonempty string.')
                return False
            return True
    print('Function input must be a string.')
    return False