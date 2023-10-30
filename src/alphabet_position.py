def alphabet_position(string):
    try:
        string = string.lower()
        alphabet = [
          'a', 'b', 'c', 'd', 'e',
          'f', 'g', 'h', 'i', 'j',
          'k', 'l', 'm', 'n', 'o',
          'p', 'q', 'r', 's', 't',
          'u', 'v', 'w', 'x', 'y',
          'z'
        ]
        position_list = []

        for character in string:
            if character in alphabet:
                position_list.append(str(alphabet.index(character) + 1))
            else:
                continue

        alphabet_string = ' '.join(position_list)
        return alphabet_string
    except (AttributeError):
        return 'Function requires a string.'
