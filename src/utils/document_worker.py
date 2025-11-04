from config.settings import RAW_DATA_DIR, PLATFORM
from datetime import datetime, timezone
import os


def process_document(document: dict, company_id: str) -> bool:
    """Process a single document: fetch its web page, store it, and download attachments."""
    try:
        # Importa funzioni necessarie localmente per evitare dipendenze circolari
        from utils.scraping_utils import store_web_page, get_attachments_url_list, download_attachment, store_metadata_debug, get_web_page, get_document_metadata

        # Build document metadata dictionary
        metadata = get_document_metadata(document)
        metadata["company_id"] = company_id

        url = metadata.get("url")
        if not url:
            raise ValueError("Document URL is missing")

        wp = get_web_page(url)

        company_name_no_space = "_".join(metadata.get("company_name").replace(" ", "_").split())
        filing_date_str = metadata.get("filing_date")
        document_id = metadata.get('document_id')
        file_type = metadata.get('file_type')

        document_folder = os.path.join(RAW_DATA_DIR, PLATFORM, f"{company_id}_{company_name_no_space}", f"{file_type}" , f"{filing_date_str}_{document_id}")

        #Transform filing_date in timestamp format

        if filing_date_str:
            try:
                filing_date = datetime.strptime(filing_date_str, "%Y%m%d").replace(tzinfo=timezone.utc)

                metadata["filing_date"] = filing_date

            except ValueError as e:
                print(f"Error parsing filing_date: {e}")

        wp_filename = "wp.html"
        wp_path = os.path.join(document_folder, wp_filename)

        relative_path = os.path.relpath(wp_path, RAW_DATA_DIR)
        
        #metadata["file_path"] = relative_path
        
        
        from utils.path_utils import convert_path_to_linux_format
        metadata["file_path"] = convert_path_to_linux_format(relative_path)

        try:
            store_web_page(wp, wp_path)
        except Exception as e:
            print(f"Error storing web page: {e}")   
            return

        filing_date_str_with_scores = filing_date.strftime("%Y-%m-%d")
        metadata["file_name"] = f"{file_type} - {company_name_no_space} - [{filing_date_str_with_scores}]"

        att_list = get_attachments_url_list(wp)
        import concurrent.futures

        if att_list and len(att_list) > 0:
            def process_attachment(att):
                try:
                    att_filename = "_".join(att.split("/")[-1].split("_")[1:])
                    for x, y in [("%20", "_"), (":", "_"), ("?", "_"), ("&", "_"), ("=", "_")]:
                        att_filename = att_filename.replace(x, y)
                    att_path = os.path.join(document_folder, att_filename)
                    download_attachment(att, att_path)

                    relative_att_path = os.path.relpath(att_path, RAW_DATA_DIR)
                    return relative_att_path
                except Exception as e:
                    print(f"Error downloading attachment {att}, {att_path}: {e}")
                    return None

            with concurrent.futures.ThreadPoolExecutor(max_workers=4) as executor:
                results = list(executor.map(process_attachment, att_list))

            # Filter out None results and store relative paths in metadata
            metadata["supporting_file_paths"] = [path for path in results if path]
        else:
            # No attachments found
            metadata["supporting_file_paths"] = None

        metadata["updated_at"] = datetime.now(timezone.utc)
        #store_metadata_debug(metadata, document_folder) 
        return metadata
    except Exception as e:
        print(f"Error processing document: {e}")
        return None