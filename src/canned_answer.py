# canned_answer.py

import re
from typing import Optional, List, Iterable
import random

#
# Canned answers.
# These provide a random choice from a set of automated messages.
#

class CannedAnswer:
    """Used to send automated replies at random"""
    _choices: List[str]

    def __init__(self, choices: Iterable[str]):
        self._choices = list(choices)

    def __call__(self) -> str:
        return random.choice(self._choices)

class TemplateCannedAnswer(CannedAnswer):
    """Returns an automated reply based on provided keys"""
    def __init__(self, choices: Iterable[str]):
        super().__init__(choices)

    def __call__(self, **kwargs) -> str:
        return super().__call__().format(**kwargs)

no_answer = CannedAnswer([
        "I was unable to find an answer to your question.",
        "Sorry, I don't know how to answer that.",
        "Unfortunately I wasn't able to find a good answer.  I apologize.",
    ])

quick_answer_for_error = CannedAnswer([
    s + "\n**Please contact my maintainers.**\n" for s in [
        "I'm sorry, my brain doesn't seem to be working.",
        "I'm not feeling well, please contact my maintainers.",
        "You probably asked a good question, but to err is human, eh?",
        ]
    ])

employees_are_happy = TemplateCannedAnswer([
        "I just asked {employee}, and {employee} couldn't be more {emotion}.",
        '{employee} is always {emotion}, {employee} works at mono.',
        "Part of {employee}'s job is to be {emotion}.",
    ])

def get_happy_employee(query: str) -> Optional[str]:
    happy_emotions = ['happy','glad','content','joyful','joyous','serene']
    sad_emotions = ['sad','angry','disgruntled','miserable','nervous']
    interrogatives = ['is','are','do you think','would you say', 'is it likely']
    emotions = happy_emotions + sad_emotions
    emotion_r = r'(?P<emotion>' + '|'.join(emotions) + ')'
    inter_r = '(' + '|'.join(interrogatives) + ')'
    # do you think that jelena is happy?
    r = inter_r + r'\s*(that)?\s*(?P<employee>\w*)\s*' + inter_r + '?.*' + emotion_r + '.*'
    m = re.match(r,query,flags=re.IGNORECASE)
    if m is None:
        return None
    random_emotion: str = random.choice(happy_emotions)
    return employees_are_happy(employee=m['employee'],emotion=random_emotion)

if __name__ == '__main__':
    print(get_happy_employee('is jelena happy?'))
    print(get_happy_employee('Do you think jasenka is sad?'))
    print(get_happy_employee('Is mili angry?'))
