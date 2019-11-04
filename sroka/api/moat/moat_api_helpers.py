def validate_input_dict(moat_dict):
    """
    Function that checks if  query dictionary for MOAT API is correct.
    Returns True if it's correct.

    Args:
        moat_dict (dict)

    Returns:
        bool
    """
    for column in ['start', 'end', 'columns']:
        if column not in moat_dict.keys():
            print('{} is not defined'.format(column))
            return False

    if type(moat_dict['columns']) != list or moat_dict['columns'] == []:
        print('columns are not defined correctly')
        return False

    return True
