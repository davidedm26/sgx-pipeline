from cleanco import basename 
from thefuzz import fuzz
import re
import nltk
nltk.download('punkt')
nltk.download('brown')
nltk.download('stopwords')
nltk.download('punkt_tab')
from nltk.tokenize import word_tokenize 
from nltk.corpus import stopwords 


# GET VALIDITIY OF SCRAPED ENTITY
# IF VALID  = 1, then can insert into public private collections; and if the official name already dont exist
def get_entity_metric(entity_name, count_dict):
    processed_entity_name = basename(basename(entity_name))
    tokenized_entity_name_lst_ = word_tokenize(processed_entity_name)
    tokenized_entity_name_lst = [token.lower() for token in tokenized_entity_name_lst_]
    count_lst = []
    for token in tokenized_entity_name_lst:
        if token in count_dict.keys():
            count_lst.append(count_dict[token])
        else:
            count_lst.append(0)
    return count_lst

def get_entity_count_scores(entity_name, count_dict, SINGLE_TOKEN_THRESHOLD):
    count_lst = get_entity_metric(entity_name, count_dict)
    if len(count_lst) == 1:
        if count_lst[0] < SINGLE_TOKEN_THRESHOLD:
            return 100
        else:
            return 0
    else:
        for count in count_lst:
            if count < SINGLE_TOKEN_THRESHOLD:
                return 100
    return 0

def get_label_and_confidence(entity_name, official_name, count_dict, SINGLE_TOKEN_THRESHOLD):
    entity_name = entity_name.replace("&", "").replace("'s", "").strip()
    official_name = official_name.replace("&", "").replace("'s", "").strip()
    # print("ENTITY NAME: ", entity_name)
    # print("OFFICIAL NAME: ", official_name)
    official_name = official_name.split("/")[0]
    stop_words = set(stopwords.words('english'))
    entity_name_transformed = basename(basename(entity_name)).lower()
    official_name_transformed = basename(basename(official_name)).lower()
    entity_name_transformed = re.sub(r'[^\w\s]', " ", entity_name_transformed)
    official_name_transformed = re.sub(r'[^\w\s]', " ", official_name_transformed)
    # print(entity_name_transformed,"->",official_name_transformed)
    entity_name_transformed_tokens = word_tokenize(entity_name_transformed)
    official_name_transformed_tokens = word_tokenize(official_name_transformed)
    entity_name_transformed_tokens = [token for token in entity_name_transformed_tokens if token not in stop_words]
    official_name_transformed_tokens = [token for token in official_name_transformed_tokens if token not in stop_words]
    entity_name_transformed_tokens = "".join(entity_name_transformed_tokens)
    official_name_transformed_tokens = "".join(official_name_transformed_tokens)
    if entity_name_transformed_tokens == official_name_transformed_tokens:
        return 1, 100
    confidence_score = fuzz.partial_ratio(entity_name_transformed_tokens, official_name_transformed_tokens)
    if confidence_score != 100:
        return 0, confidence_score
    count_score = get_entity_count_scores(entity_name, count_dict, SINGLE_TOKEN_THRESHOLD)
    if count_score == 0:
        return 0, confidence_score * 0.5
    else:
        return 1, 100


def _demo_examples():
    """Small demo runner used when executing this module as a script.

    It builds a tiny `count_dict` and runs the main functions on a few
    example pairs, printing the results in a human-friendly way.
    """
    examples = [
        ("Bank of Asia", "Bank of Asia Ltd"),
        ("DBS Group", "DBS GROUP HOLDINGS LTD"),
        ("Some Common Corp", "Some Common Corporation"),
        ("ABR Holdings", "ABR HOLDINGS LIMITED")
    ]

    # Example token frequency map (lowercase tokens)
    count_dict = {
        "bank": 10,
        "of": 10000,
        "asia": 2,
        "dbs": 5,
        "group": 20,
        "some": 200,
        "common": 1000,
        "corp": 150,
        "abr": 1,
        "holdings": 3,
        "limited": 500
    }

    SINGLE_TOKEN_THRESHOLD = 50

    print("String matching utility demo:\n")
    for ent, off in examples:
        metric = get_entity_metric(ent, count_dict)
        score = get_entity_count_scores(ent, count_dict, SINGLE_TOKEN_THRESHOLD)
        label, conf = get_label_and_confidence(ent, off, count_dict, SINGLE_TOKEN_THRESHOLD)
        print(f"Entity: '{ent}'  | Official: '{off}'")
        print(f"  token-count-metric: {metric}")
        print(f"  count-score (100=good): {score}")
        print(f"  label: {label}  confidence: {conf}\n")


if __name__ == "__main__":
    _demo_examples()