import random
import string

SURNAME_LENGTH:int = 6

def gen_surname() -> string:
    return ''.join(random.choice(string.ascii_uppercase + string.digits)
                   for _ in range(SURNAME_LENGTH))