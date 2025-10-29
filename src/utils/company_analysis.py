from pathlib import Path
import sys
import json
ROOT_PATH = Path(__file__).resolve().parent.parent.parent
SRC_PATH = ROOT_PATH / "src"
sys.path.append(str(ROOT_PATH))
sys.path.append(str(SRC_PATH))

from utils.scraping_utils import request_documents_count

company_list = [
        "DBS BANK LTD.",
        "DBS BANK LTD. (ACTING THROUGH ITS LONDON BRANCH)",
        "DBS BANK LTD. (ACTING THROUGH ITS REGISTERED OFFICE IN SINGAPORE)",
        "DBS BANK LTD., AUSTRALIA BRANCH",
        "DBS BANK LTD., HONG KONG BRANCH ",
        "DBS CAPITAL FUNDING CORPORATION",
        "DBS CAPITAL FUNDING II CORPORATION",
        "DBS GROUP HOLDINGS LTD",
        "DBS TRUSTEE LIMITED",
        "DBS TRUSTEE LIMITED (IN ITS CAPACITY AS TRUSTEE OF ASCOTT RESIDENCE TRUST)",
        "DBS TRUSTEE LIMITED (IN ITS CAPACITY AS TRUSTEE OF CAPITALAND ASCOTT REAL ESTATE INVESTMENT TRUST)",
        "DBS TRUSTEE LIMITED IN ITS CAPACITY AS TRUSTEE OF EAGLE HOSPITALITY REAL ESTATE INVESTMENT TRUST",
        "DBS TRUSTEE LIMITED (IN ITS CAPACITY AS TRUSTEE OF IREIT GLOBAL)",
        "DBS TRUSTEE LIMITED (IN ITS CAPACITY AS TRUSTEE OF MAPLETREE INDUSTRIAL TRUST)",
        "DBS TRUSTEE LIMITED (IN ITS CAPACITY AS TRUSTEE OF MAPLETREE NORTH ASIA COMMERCIAL TRUST)",
        "DBS TRUSTEE LIMITED (IN ITS CAPACITY AS TRUSTEE OF PARAGON REIT)",
        "DBS TRUSTEE LIMITED (IN ITS CAPACITY AS TRUSTEE OF SOILBUILD BUSINESS SPACE REIT)",
        "DBS TRUSTEE LIMITED (IN ITS CAPACITY AS TRUSTEE OF SPH REIT)",
        "DBS TRUSTEE LIMITED (TRUSTEE OF LENDLEASE GLOBAL COMMERCIAL REIT)"
]

def get_all_time_results(company_list):
    """Retrieve the all-time number of results for each company in the list.

    Args:
        company_list (list): List of company names.

    Returns:
        dict: A dictionary with company names as keys and their all-time result counts as values.
    """
    results = {}
    for company in company_list:
        try:
            count = request_documents_count(company_name=company, periodstart="20051030_160000", periodend="20251028_155959")
            if count is not None:
                results[company] = count
            else:
                results[company] = "Error retrieving count"
        except Exception as e:
            results[company] = f"Error: {e}"
    return results

def process_companies_and_save_results(input_file: str, output_file: str):
    """Process companies from a JSON file and save their results to a new JSON file.

    Args:
        input_file (str): Path to the input JSON file containing company names.
        output_file (str): Path to the output JSON file to save results.
    """
    try:
        # Load company data from the input file
        with open(input_file, "r", encoding="utf-8") as file:
            data = json.load(file)

        # Consider only the last 80 companies
        companies = data.get("data", [])
        results = {}

        # Load existing results if the output file exists
        existing_results = {}
        if Path(output_file).exists():
            with open(output_file, "r", encoding="utf-8") as file:
                existing_results = json.load(file)

        # Merge new results with existing results
        for company in companies:
            try:
                count = request_documents_count(company_name=company, periodstart="20051030_160000", periodend="20251028_155959")
                print(f"Processed {company}: {count}")
                existing_results[company] = count if count is not None else "Error retrieving count"
            except Exception as e:
                existing_results[company] = f"Error: {e}"

        # Save merged results to the output file
        with open(output_file, "w", encoding="utf-8") as file:
            json.dump(existing_results, file, indent=4)

        print(f"Results saved to {output_file}")

    except Exception as e:
        print(f"Error processing companies: {e}")

def calculate_sums(doc_count_file, company_list_file):
    """
    Calcola la somma di tutte le voci in doc_count_file e la somma delle voci corrispondenti alle aziende in company_list_file.

    :param doc_count_file: Percorso del file JSON contenente i conteggi dei documenti per azienda.
    :param company_list_file: Percorso del file JSON contenente l'elenco delle aziende.
    :return: Un dizionario con le somme calcolate.
    """
    try:
        # Leggi il file JSON con i conteggi dei documenti
        with open(doc_count_file, 'r', encoding='utf-8') as f:
            doc_counts = json.load(f)

        # Leggi il file JSON con l'elenco delle aziende
        with open(company_list_file, 'r', encoding='utf-8') as f:
            company_data = json.load(f)

        # Estrai l'elenco delle aziende
        company_list = company_data.get("companyName", [])
        #print(company_list)

        # Calcola la somma di tutte le voci
        total_sum = sum(doc_counts.values())

        # Calcola la somma delle voci corrispondenti alle aziende elencate
        listed_sum = sum(value for company, value in doc_counts.items() if company.startswith("[LISTED]"))

        return {
            "total_sum": total_sum,
            "listed_sum": listed_sum
        }

    except (FileNotFoundError, json.JSONDecodeError) as e:
        print(f"Errore durante la lettura dei file JSON: {e}")
        return None

def complete_doc_counts(doc_count_file):
    """
    Completa i valori mancanti o errati nel file dei conteggi dei documenti.

    :param doc_count_file: Percorso del file JSON contenente i conteggi dei documenti per azienda.
    """
    try:
        # Leggi il file JSON con i conteggi dei documenti
        with open(doc_count_file, 'r', encoding='utf-8') as f:
            doc_counts = json.load(f)

        # Identifica le aziende con valori nulli o stringhe di errore
        for company, value in doc_counts.items():
            if value is None or isinstance(value, str):
                try:
                    # Richiama la funzione per ottenere il conteggio aggiornato
                    updated_count = request_documents_count(company_name=company, periodstart="20051030_160000", periodend="20251028_155959")
                    print(f"Aggiornato {company}: {updated_count}")
                    doc_counts[company] = updated_count if updated_count is not None else "Error retrieving count"
                except Exception as e:
                    doc_counts[company] = f"Error: {e}"

        # Salva i conteggi aggiornati nel file
        with open(doc_count_file, 'w', encoding='utf-8') as f:
            json.dump(doc_counts, f, indent=4)

        print(f"File {doc_count_file} aggiornato con successo.")

    except (FileNotFoundError, json.JSONDecodeError) as e:
        print(f"Errore durante la lettura o scrittura del file JSON: {e}")

def add_listed_prefix(doc_count_file, company_list_file):
    """
    Aggiunge il prefisso '[LISTED]' ai nomi delle aziende in doc_count_file
    che compaiono in company_list_file.

    :param doc_count_file: Percorso del file JSON contenente i conteggi dei documenti per azienda.
    :param company_list_file: Percorso del file JSON contenente l'elenco delle aziende.
    """
    try:
        # Leggi il file JSON con i conteggi dei documenti
        with open(doc_count_file, 'r', encoding='utf-8') as f:
            doc_counts = json.load(f)

        # Leggi il file JSON con l'elenco delle aziende
        with open(company_list_file, 'r', encoding='utf-8') as f:
            company_data = json.load(f)

        # Estrai l'elenco delle aziende
        company_list = company_data.get("companyName", [])

        # Aggiungi il prefisso '[LISTED]' ai nomi delle aziende corrispondenti
        updated_doc_counts = {}
        for company, value in doc_counts.items():
            if company in company_list:
                updated_doc_counts[f"[LISTED] {company}"] = value
            else:
                updated_doc_counts[company] = value

        # Salva i conteggi aggiornati nel file
        with open(doc_count_file, 'w', encoding='utf-8') as f:
            json.dump(updated_doc_counts, f, indent=4)

        print(f"Prefisso '[LISTED]' aggiunto alle aziende corrispondenti in {doc_count_file}.")

    except (FileNotFoundError, json.JSONDecodeError) as e:
        print(f"Errore durante la lettura o scrittura dei file JSON: {e}")

# Example usage
if __name__ == "__main__":
    #results = get_all_time_results(company_list)
    #for company, count in results.items():
    #    print(f"{company}: {count}")
    input_path = "B:\\Workspace\\sgx-pipeline\\data\\company_list.json"
    output_path = "B:\\Workspace\\sgx-pipeline\\data\\all_company_doc_count.json"
    #process_companies_and_save_results(input_path, output_path)
    doc_count_path = "b:/Workspace/sgx-pipeline/data/all_company_doc_count.json"
    company_list_path = "b:/Workspace/sgx-pipeline/data/listed_company_list.json"

    #complete_doc_counts(doc_count_path)

    results = calculate_sums(doc_count_path, company_list_path)
    if results:
        print(f"Somma totale: {results['total_sum']}")
        print(f"Somma delle aziende elencate: {results['listed_sum']}")

    #add_listed_prefix(doc_count_path, company_list_path)
