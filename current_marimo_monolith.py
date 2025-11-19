import marimo

__generated_with = "0.17.2"
app = marimo.App(width="columns")

with app.setup:
    import asyncio
    from collections import defaultdict
    import difflib
    import functools
    from pathlib import Path
    import pickle
    import re
    import time
    from time import sleep

    import altair as alt
    import httpx
    from Levenshtein import ratio
    import marimo as mo
    import polars as pl
    from rich import print
    from selectolax.parser import HTMLParser
    import xmltodict

    # INPUT / SETTINGS
    publications_path = "works.pkl"
    orgs_path = "openaire_cris_orgunits.parquet"
    persons_path = "openaire_cris_persons.parquet"
    oils_data_path = "Elsevier 2022-2024 Usage en publicaties.xlsx"

    pub_path = mo.ui.file_browser(
        label="Select publications data file",
        initial_path=Path().cwd(),
        filetypes=[".pkl"],
        multiple=False,
    )
    org_path = mo.ui.file_browser(
        label="Select organizations data file",
        initial_path=Path().cwd(),
        filetypes=[".parquet"],
        multiple=False,
    )
    pers_path = mo.ui.file_browser(
        label="Select persons data file",
        initial_path=Path().cwd(),
        filetypes=[".parquet"],
        multiple=False,
    )
    oils_path = mo.ui.file_browser(
        label="Select OILS data file",
        initial_path=Path().cwd(),
        filetypes=[".parquet"],
        multiple=False,
    )

    run_openalex_queries = mo.ui.checkbox(
        label="Retrieve data from OpenAlex API (~5 seconds per 50 items)"
    )
    run_people_page_queries = mo.ui.checkbox(
        label="Retrieve data from People Pages (~5 seconds per persons)"
    )
    merge_with_oils = mo.ui.checkbox(
        label="Merge with OILS dataset (Elsevier publications with usage data)"
    )
    use_titles = mo.ui.checkbox(
        label="When retrieving from OpenAlex, retry unfound items using title search"
    )
    verbose_people_page_retrieval = mo.ui.checkbox(
        label="Enable verbose logging for People Page retrieval"
    )
    filter_years = mo.ui.range_slider(start=2000, stop=2026, step=1, value=[2020, 2025])
    filter_faculty = mo.ui.multiselect(
        label="Select faculties to filter on (optional)",
        options=[
            "tnw",
            "eemcs",
            "et",
            "bms",
            "itc",
        ],
    )
    start_button = mo.ui.run_button(
        kind="success", label="Click to start processing", full_width=True
    )

    def timing_decorator(func):
        """
        A decorator that prints the function name and execution time.
        """

        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            start_time = time.perf_counter()
            result = func(*args, **kwargs)
            end_time = time.perf_counter()
            elapsed_time = end_time - start_time
            print(f"[{func.__name__} | {elapsed_time:.4f} s]")
            return result

        return wrapper


@app.cell
def _(add_openalex_to_df, enrich_employee_data):
    @timing_decorator
    async def full_pipeline(
        publications_path: str,
        orgs_path: str,
        persons_path: str,
        oils_data_path: str,
        run_people_page_queries: bool,
        run_openalex_queries: bool,
        use_titles: bool,
        filter_years: list[int] | None = None,
        filter_publisher: list[str] | None = None,
        filter_faculty: list[str] | None = None,
        merge_with_oils: bool = False,
        input_clean_data: pl.DataFrame | None = None,
    ) -> None:
        """
        Full pipeline to process Pure data, enrich with People Page and OpenAlex data, merge with OILS data, and prepare for export.
        Parameters:

        Core data:
            Files exported by MUS OAI-PMH scraper (paths / strs):
            - publications_path: path to Pure publications data file (pickle)
            - orgs_path: path to Pure organizations data file (parquet)
            - persons_path: path to Pure persons data file (parquet)

            OR cleaned data from earlier run (pl.DataFrame):
            - input_clean_data: cleaned Pure publications data

        OILS data (path/str):
        - oils_data_path: path to OILS data file (excel)

        Toggles (bool):
        - run_people_page_queries: whether to enrich person data with People Page
        - run_openalex_queries: whether to enrich publication data with OpenAlex
        - merge_with_oils: whether to merge the OILS dataset into the output
        - use_titles: whether to use title search in addition to DOI when querying OpenAlex

        Filters (list[str]|list[int]):
        - filter_years: list with two integers [start_year, end_year] to filter publications by year, e.g. [2018, 2023]
        - filter_publisher: list of publisher names to filter publications by publisher, e.g. ['Elsevier', 'IEEE']
        - filter_faculty: list of lowercased faculty abbreviations to filter publications by faculty, e.g. ['tnw', 'eemcs']

        Returns (pl.DataFrame): final merged, enriched, and filtered dataframe

        """

        if not isinstance(input_clean_data, pl.DataFrame):
            print(
                f"Data from: {publications_path}, {orgs_path}, {persons_path}, {oils_data_path}"
            )
        else:
            print(
                f"Data from provided cleaned DataFrame with {input_clean_data.height} rows"
            )

        print(
            f"Filters: years={filter_years}, publisher={filter_publisher}, faculty={filter_faculty} "
        )
        print(
            f"Queries: People Page={run_people_page_queries}, OpenAlex={run_openalex_queries}"
        )
        print(
            f"merge_with_oils={merge_with_oils}, input_clean_data provided={isinstance(input_clean_data, pl.DataFrame)}"
        )

        oils_data = pl.read_excel(oils_data_path, sheet_name="Bron Publ").rename({
            "DOIs (Digital Object Identifiers) link": "doi"
        })

        if not isinstance(input_clean_data, pl.DataFrame):
            # LOAD
            pure_publications_ingest, pure_orgs_ingest, pure_persons_ingest = (
                load_pure_data_from_files(publications_path, orgs_path, persons_path)
            )
            ut_publications_cleaned = clean_publications(pure_publications_ingest)
            ut_person_data = clean_and_enrich_persons_data(
                pure_persons_ingest, pure_orgs_ingest
            )
            selected_items = ut_publications_cleaned
        else:
            selected_items = input_clean_data

        if filter_years:
            selected_items = selected_items.filter(
                (pl.col("publication_year") >= filter_years[0])
                & (pl.col("publication_year") <= filter_years[1])
            )

        if filter_publisher:
            selected_items = selected_items.filter(
                pl.col("publisher").is_in(filter_publisher)
            )

        if not isinstance(input_clean_data, pl.DataFrame):
            # ENRICH
            if not run_people_page_queries and not filter_faculty:
                mo.output.append(
                    mo.md(
                        "attention | Retrieval of People Page data has been skipped because checkbox `run_people_page_queries` is unchecked."
                    )
                )
            else:
                ut_person_data = await enrich_employee_data(ut_person_data)

            ut_publications_enriched = join_authors_and_publications(
                ut_person_data, selected_items
            )

            if filter_faculty:
                gathered = []

                for fac in filter_faculty:
                    if fac.lower() not in ut_publications_enriched.columns:
                        mo.output.append(
                            mo.md(
                                f"warning | Faculty abbreviation `{fac}` not found in data columns. Available faculty columns: {[col for col in ut_publications_enriched.columns if col in ['tnw', 'eemcs', 'et', 'bms', 'itc']]}"
                            )
                        )
                    else:
                        gathered.append(
                            ut_publications_enriched.filter(pl.col(fac.lower()) == True)
                        )

                if len(gathered) == 0:
                    mo.output.append(
                        mo.md(
                            "warning | No valid faculty abbreviations found for filtering. Proceeding without faculty filter."
                        )
                    )

                if len(gathered) == 1:
                    ut_publications_enriched = gathered[0]

                else:
                    ut_publications_enriched = pl.concat(gathered).unique()
        else:
            ut_publications_enriched = selected_items

        if run_openalex_queries:
            if isinstance(input_clean_data, pl.DataFrame):
                if "publication_year_oa" in input_clean_data:
                    ut_publications_enriched_plus_oa = ut_publications_enriched
                else:
                    ut_publications_enriched_plus_oa = add_openalex_to_df(
                        df=ut_publications_enriched,
                        doi_col="doi",
                        use_titles=use_titles,
                        title_col="title",
                    )
            else:
                ut_publications_enriched_plus_oa = add_openalex_to_df(
                    df=ut_publications_enriched,
                    doi_col="doi",
                    use_titles=use_titles,
                    title_col="title",
                )
        else:
            ut_publications_enriched_plus_oa = ut_publications_enriched

        if merge_with_oils:
            ut_publications_enriched_plus_oa = merge_oils_with_all(
                oils_data, ut_publications_enriched_plus_oa
            ).with_columns(
                pl.concat_str([pl.lit("https://doi.org/"), pl.col("doi")]).alias(
                    "doi_url"
                ),
            )

        pure_oils_oa_df = extract_author_and_funder_names(
            ut_publications_enriched_plus_oa
        )
        pure_oils_oa_df = add_missing_affils(pure_oils_oa_df)

        return pure_oils_oa_df

    return (full_pipeline,)


@app.cell
def _():
    note = r"""
        # for finding authors that are missing faculty affiliations
        merged_df.explode("pure_authors_names").group_by(
            "pure_authors_names", "faculty_abbr"
        ).count().sort("count", descending=True).with_columns(
            pl.concat_str(
                [
                    pl.lit("https://people.utwente.nl/overview?query="),
                    pl.col("pure_authors_names")
                    .str.replace_all(" ", "%20")
                    .str.replace_all("\.", "%20"),
                ]
            ).alias("pp_url")
        )
        """


@app.function
@timing_decorator
def load_pure_data_from_files(
    publications_path: str | None = None,
    orgs_path: str | None = None,
    persons_path: str | None = None,
) -> tuple[pl.DataFrame, pl.DataFrame, pl.DataFrame]:
    pure_publications = (
        load_pure_publications_from_pickle(publications_path)
        if str(publications_path).endswith(".pkl")
        else pl.read_parquet(publications_path)
    )
    pure_orgs = pl.read_parquet(orgs_path)
    pure_persons = pl.read_parquet(persons_path)
    return pure_publications, pure_orgs, pure_persons


@app.function
@timing_decorator
def load_pure_publications_from_pickle(pickle_path: str | Path) -> pl.DataFrame:
    from collections import Counter
    import pickle

    # load in pure publication data from pickle and
    # coerce all fields to a common type so we can create a df without schema issues

    def handle_list_of_dicts(v, fields: list[str]):
        # list of dicts with only string values, no further nesting
        if not v:
            return [dict.fromkeys(fields)]
        if not isinstance(v, list):
            v = [v]
        new = []
        for el in v:
            if not isinstance(el, dict):
                continue
            cur_dict = {}
            for f in fields:
                if f not in el:
                    cur_dict[f] = None
                else:
                    cur_dict[f] = el[f]

            new.append(cur_dict)
        return new

    with Path(pickle_path).open("rb") as f:
        works_data = pickle.load(f)

    new_works = []
    null_counts = Counter()
    for item in works_data:
        new_item = {}
        for k, v in item.items():
            match k:
                case (
                    "title"
                    | "subtitle"
                    | "publishers"
                    | "license"
                    | "keywords"
                    | "originates_from"
                ):
                    if not v:
                        new_item[k] = []
                    else:
                        new_item[k] = [v] if not isinstance(v, list) else v
                case "file_locations":
                    new_item[k] = handle_list_of_dicts(
                        v, ["type", "title", "uri", "mime_type", "size", "access"]
                    )
                case "isbn":
                    new_item[k] = handle_list_of_dicts(v, ["medium", "value"])
                case "authors":
                    new_item[k] = handle_list_of_dicts(
                        v, ["internal_repository_id", "family_names", "first_names"]
                    )
                case "issn":
                    new_item[k] = handle_list_of_dicts(v, ["Print", "Online"])
                case "presented_at" | "editors":
                    continue
                case "references":
                    new_item[k] = handle_list_of_dicts(
                        v,
                        [
                            "internal_repository_id",
                            "peer_reviewed",
                            "publication_category",
                            "type",
                            "title",
                        ],
                    )
                case _:
                    new_item[k] = v if v else None

            if not new_item[k]:
                null_counts[k] += 1
        new_works.append(new_item)

    return pl.from_dicts(new_works, infer_schema_length=200000, strict=False)


@app.function
@timing_decorator
def clean_and_enrich_persons_data(person_data, org_data):
    full_faculty_names = [
        "Faculty of Science and Technology",
        "Faculty of Engineering Technology",
        "Faculty of Electrical Engineering, Mathematics and Computer Science",
        "Faculty of Behavioural, Management and Social Sciences",
        "Faculty of Geo-Information Science and Earth Observation",
        "TechMed Centre",
        "Digital Society Institute",
        "MESA+ Institute",
    ]
    short_faculty_names = ["tnw", "et", "eemcs", "bms", "itc", "techmed", "dsi", "mesa"]

    if (
        "family_names" not in person_data.columns
        or "first_names" not in person_data.columns
    ):
        mo.output.append(
            mo.md(
                f"""warning | No `family_names` & `first_names` column found

                The provided person_data does not contain 'family_names' or 'first_names' columns. Cannot add people_page_urls.

                Found columns:
                {person_data.columns}
                """
            )
        )
    else:
        people_page_url = "https://people.utwente.nl/overview?query="
        person_data = person_data.rename({
            "internal_repository_id": "pure_id",
            "family_names": "last_name",
            "first_names": "first_name",
        }).drop([
            "scopus_affil_id",
            "researcher_id",
            "isni",
            "cris-id",
            "uuid",
            "uri",
            "url",
        ])

        names_for_people_page = (
            person_data.select(["last_name", "first_name", "pure_id"])
            .drop_nulls()
            .to_dicts()
        )
        people_page_urls = [
            {
                "pure_id": pers["pure_id"],
                "url": "".join([
                    people_page_url,
                    pers["first_name"],
                    "%20",
                    pers["last_name"],
                ]).replace(" ", "%20"),
            }
            for pers in names_for_people_page
        ]
        person_data = person_data.join(
            pl.from_dicts(people_page_urls), on="pure_id", how="left"
        )

    if any(["affiliations" not in person_data.columns]):
        mo.output.append(
            mo.md(
                """warning | No `affiliations` column found

                The provided person_data does not contain an 'affiliations' column. Cannot proceed with cleaning and enriching persons data.
                """
            )
        )
        return person_data

    found_unique_affils = (
        person_data.filter(pl.col("affiliations").is_not_null())
        .select("affiliations")
        .explode("affiliations")
        .unnest("affiliations")
        .unique(["name", "internal_repository_id"])
    )

    clean_org_data = (
        org_data.filter(pl.col("part_of").is_not_null())
        .with_columns(pl.col("part_of").struct.field("name").alias("parent_org"))
        .drop(["identifiers", "part_of", "acronym", "url"])
        .with_columns(
            pl.col("parent_org").str.replace_many(
                full_faculty_names, short_faculty_names
            ),
            pl.col("name").str.replace_many(full_faculty_names, short_faculty_names),
        )
        .with_columns(
            pl.when(pl.col("name").is_in(short_faculty_names))
            .then(pl.col("name"))
            .otherwise(pl.col("parent_org"))
            .alias("parent_org")
        )
    )

    enhanced_org_data = (
        found_unique_affils.join(
            clean_org_data,
            left_on="internal_repository_id",
            right_on="internal_repository_id",
            how="left",
        )
        .drop_nulls()
        .with_columns([
            pl.when(pl.col("parent_org").eq(col))
            .then(pl.lit(True))
            .otherwise(pl.lit(False))
            .alias(col)
            for col in short_faculty_names
        ])
    )

    pure_persons_with_affil_ids = (
        person_data.with_columns(
            pl.col("affiliations")
            .list.eval(pl.element().struct.field("internal_repository_id"))
            .list.drop_nulls()
            .alias("affiliation_ids")
        )
        .explode("affiliation_ids")
        .join(
            enhanced_org_data,
            left_on="affiliation_ids",
            right_on="internal_repository_id",
            how="left",
        )
        .group_by("pure_id")
        .agg([pl.col(col).any().alias(col) for col in short_faculty_names])
    )

    return (
        person_data.join(pure_persons_with_affil_ids, on="pure_id", how="left")
        .with_columns(
            pl.col("affiliations")
            .list.eval(pl.element().struct.field("name"))
            .list.drop_nulls()
            .alias("affiliation_names_pure"),
            pl.col("affiliations")
            .list.eval(pl.element().struct.field("internal_repository_id"))
            .list.drop_nulls()
            .alias("affiliation_ids_pure"),
        )
        .with_columns(
            pl.col("affiliation_ids_pure")
            .list.contains("491145c6-1c9b-4338-aedd-98315c166d7e")
            .alias("is_ut")
        )
        .drop("affiliations")
        .filter(pl.col("is_ut"))
        .drop("is_ut")
    )


@app.function
def parse_organization_details(tree: HTMLParser) -> list[dict[str, any]]:
    """Parses the detailed organization structure from a person's profile page."""

    def parse_org_text(text: str, split=False) -> dict[str, str]:
        """Extracts name and abbreviation from text like 'Name (ABBR)'."""
        match = re.search(r"(.+?)\s*\(([^)]+)\)$", text)
        if match:
            abbr = match.group(2).strip()
            if abbr and split:
                abbr = abbr.split("-")[-1].strip()
            return {"name": match.group(1).strip(), "abbr": abbr}
        return {"name": text.strip(), "abbr": None}

    all_headings = tree.css("h2.heading2")
    org_heading = None
    for heading in all_headings:
        if heading.text(strip=True) == "Organisations":
            org_heading = heading
            break

    if not org_heading:
        return None

    org_widget = org_heading.next
    if not org_widget or "widget-linklist" not in org_widget.attributes.get(
        "class", ""
    ):
        return None

    list_items = org_widget.css("li.widget-linklist__item")
    organizations = []
    current_org = {}

    for item in list_items:
        text_node = item.css_first("span.widget-linklist__text")
        if not text_node:
            continue
        text = text_node.text(strip=True)

        item_class = item.attributes.get("class", "")
        if "widget-linklist__item--level1" in item_class:
            if current_org:
                organizations.append(current_org)

            current_org = {
                "faculty": parse_org_text(text),
                "department": {"name": None, "abbr": None},
                "group": {"name": None, "abbr": None},
            }
        elif "widget-linklist__item--level2" in item_class and current_org:
            current_org["department"] = parse_org_text(text, split=True)
        elif "widget-linklist__item--level3" in item_class and current_org:
            current_org["group"] = parse_org_text(text, split=True)

    if current_org:
        organizations.append(current_org)

    return organizations if organizations else None


@app.cell
def _(verbose):
    async def fetch_organization_details(
        client: httpx.AsyncClient, url: str
    ) -> list[dict[str, any]]:
        """Fetches the HTML of a person's page and parses the organization details."""
        if not url:
            return None
        try:
            response = await client.get(url, timeout=10)
            response.raise_for_status()
            tree = HTMLParser(response.text)
            org_data = parse_organization_details(tree)
            if verbose:
                print(org_data)
            return org_data
        except httpx.RequestError as e:
            print(f"HTTP error for page {url}: {e}")
        except Exception as e:
            print(f"An error occurred processing page {url}: {e}")
            # print full traceback
            import traceback

            traceback.print_exc()
        return None

    return (fetch_organization_details,)


@app.function
def parse_found_name(found_name):
    """
    Parses a string in the format 'Lastname, I. (Firstname)' into its components.

    Args:
        found_name: The string to parse.

    Returns:
        A dictionary with 'first_name', 'last_name', and 'initials',
        or None if the string doesn't match the expected format.
    """
    match = re.match(r"([^,]+),\s*(.*?)\s*\((.*?)\)", found_name)
    if not match:
        return None

    last_name_part, middle_part, first_name = match.groups()

    initials = []
    particles = []
    for part in middle_part.split():
        if "." in part:
            initials.append(part)
        else:
            particles.append(part)

    initials_str = " ".join(initials)

    if particles:
        last_name = f"{' '.join(particles)} {last_name_part}"
    else:
        last_name = last_name_part

    return {
        "first_name": first_name.replace(" - ", "-"),
        "last_name": last_name.replace(" - ", "-"),
        "initials": initials_str,
    }


@app.cell
def _(verbose):
    async def fetch_employee_data(
        client: httpx.AsyncClient, first_name: str, last_name: str
    ) -> dict[str, any]:
        """
        Fetches and parses employee data from the UT people page RPC endpoint using selectolax.
        """

        first_name = first_name or ""
        last_name = last_name or ""

        last_name = last_name.replace(" - ", "-")
        if " " in first_name:
            first_name = first_name.split(" ")[0]

        query = f"{first_name} {last_name}"
        rpc_url = "https://people.utwente.nl/wh_services/utwente_ppp/rpc/"
        payload = {
            "id": 1,
            "method": "SearchPersons",
            "params": [
                {"query": query, "page": 0, "resultsperpage": 20, "langcode": "en"}
            ],
        }

        try:
            response = await client.post(rpc_url, json=payload, timeout=10)
            response.raise_for_status()
            json_response = response.json()

            if not (
                json_response.get("result")
                and json_response["result"].get("resultshtml")
            ):
                return None

            html_content = json_response["result"]["resultshtml"].replace("\\", "")
            tree = HTMLParser(html_content)

            person_tiles = tree.css("div.ut-person-tile")

            for tile in person_tiles:
                name_node = tile.css_first("h3.ut-person-tile__title")
                if not name_node:
                    continue

                found_name = name_node.text(strip=True)
                parsed_name = parse_found_name(found_name)

                found_name_final = f"{parsed_name['first_name']} {parsed_name['last_name'].replace(',', '')}"
                ratio_val = ratio(found_name_final, query, score_cutoff=0.79)
                if (ratio_val < 0.8) and (" " in parsed_name["first_name"]):
                    found_name_final = f"{parsed_name['first_name'].split(' ')[0]} {parsed_name['last_name'].replace(',', '')}"
                    ratio_val = ratio(found_name_final, query, score_cutoff=0.79)
                if (ratio_val < 0.8) and (" " in last_name):
                    query = f"{first_name} {last_name.split(' ')[0]}"
                    ratio_val = ratio(found_name_final, query, score_cutoff=0.79)
                if ratio_val < 0.8:
                    # match using only last name if if first letters of first name match
                    first_letter_query = {first_name[0]}
                    query = f"{first_name[0]} {last_name}"
                    first_letter_found = parsed_name["initials"][0]
                    found_name_final = (
                        f"{first_letter_found} {parsed_name['last_name']}"
                    )
                    if first_letter_query == first_letter_found:
                        ratio_val = ratio(
                            {parsed_name["last_name"]}, last_name, score_cutoff=0.79
                        )

                if verbose:
                    print(
                        f"{'✅' if ratio_val >= 0.8 else '❌'} {ratio_val} | {query} ~~ {found_name_final} "
                    )
                if ratio_val >= 0.8:
                    email_node = tile.css_first("div.ut-person-tile__mail span.text")
                    url_node = tile.css_first("div.ut-person-tile__profilelink a")
                    role_node = tile.css_first("div.ut-person-tile__roles")

                    orgs = [
                        org_node.text(strip=True)
                        for org_node in tile.css("div.ut-person-tile__orgs > div")
                    ]
                    result_dict = {
                        "found_name": found_name,
                        "role": role_node.text(strip=True) if role_node else None,
                        "email": email_node.text(strip=True) if email_node else None,
                        "people_page_url": url_node.attributes.get("href")
                        if url_node
                        else None,
                        "main_orgs": orgs if orgs else None,
                    }
                    return result_dict
            return None

        except httpx.RequestError as e:
            print(f"HTTP error for {query}: {e}")
        except Exception as e:
            print(f"An error occurred for {query}: {e}")

    return (fetch_employee_data,)


@app.cell
def _(fetch_employee_data, fetch_organization_details):
    @timing_decorator
    async def enrich_employee_data(df: pl.DataFrame) -> pl.DataFrame:
        """Orchestrates the two-layer data fetching and enrichment process."""

        if "first_name" not in df.columns or "last_name" not in df.columns:
            mo.output.append(
                mo.md(
                    "attention | DataFrame must contain 'first_name' and 'last_name' columns for enrichment."
                )
            )
            return df

        async with httpx.AsyncClient(limits=httpx.Limits(max_connections=5)) as client:
            search_tasks = [
                fetch_employee_data(client, row["first_name"], row["last_name"])
                for row in df.iter_rows(named=True)
            ]
            search_results = await asyncio.gather(*search_tasks)

            page_urls = [
                res.get("people_page_url") if res else None for res in search_results
            ]

            detail_tasks = [
                fetch_organization_details(client, url) for url in page_urls
            ]
            detail_results = await asyncio.gather(*detail_tasks)

        # Consolidate all data
        df_enriched = df.with_columns(
            pl.Series(
                "found_name_pp",
                [res.get("found_name") if res else None for res in search_results],
            ),
            pl.Series(
                "role", [res.get("role") if res else None for res in search_results]
            ),
            pl.Series(
                "email", [res.get("email") if res else None for res in search_results]
            ),
            pl.Series("url_pp", page_urls),
            pl.Series(
                "orgs_pp",
                [res.get("main_orgs") if res else None for res in search_results],
            ),
            pl.Series("org_details_pp", detail_results),
        )

        df_enriched_and_parsed = parse_org_details(df_enriched)
        return df_enriched_and_parsed

    return (enrich_employee_data,)


@app.function
@timing_decorator
def parse_org_details(df: pl.DataFrame) -> pl.DataFrame:
    """uses the nested dict structure in column 'org_details' to set the bools for faculties and institutes, and creates convenience cols for names and abbrs for all orgs"""

    parsing_mapping = {
        "Digital Society Institute": "dsi",
        "MESA+ Institute": "mesa",
        "TechMed Centre": "techmed",
        "Faculty of Electrical Engineering, Mathematics and Computer Science": "eemcs",
        "Faculty of Engineering Technology": "et",
        "Faculty of Behavioural, Management and Social Sciences": "bms",
        "Faculty of Science and Technology": "tnw",
        "Faculty of Geo-Information Science and Earth Observation": "itc",
    }

    # add columns to df, one per value in parsing_mapping, type bool, default false
    df = df.with_columns([
        pl.lit(False).alias(col_name) for col_name in parsing_mapping.values()
    ])
    if "org_details_pp" not in df.columns:
        mo.output.append(
            mo.md(
                " attention |  No 'org_details_pp' column found; skipping parsing of organizational details."
            )
        )
        return df
    # for each item in list col 'organization_details', match nested key [faculty][name] to keys in parsing_mapping, and set the corresponding col to true
    # if name does not match any key, leave all cols as false
    df = (
        df.with_columns(
            pl.col("org_details_pp")
            .list.eval(
                pl.element()
                .struct.field("faculty")
                .struct.field("name")
                .replace_strict(parsing_mapping, default=None)
            )
            .alias("parsed_name")
        )
        .with_columns([
            pl.col("parsed_name").list.contains(col_name).alias(col_name + "_new")
            for name, col_name in parsing_mapping.items()
        ])
        .with_columns([
            pl.col(col_name + "_new").fill_null(False)
            for col_name in parsing_mapping.values()
        ])
        .with_columns([
            (pl.col(col_name) | pl.col(col_name + "_new")).alias(col_name)
            for col_name in parsing_mapping.values()
        ])
        .drop(
            [col_name + "_new" for col_name in parsing_mapping.values()]
            + ["parsed_name"]
        )
    )

    # also extract org / institute names and abbrs into separate cols, using comma-separated strings as values in case of multiple entries

    df = df.with_columns(
        pl.col("org_details_pp")
        .list.eval(pl.element().struct.field("faculty").struct.field("name"))
        .list.filter(~pl.element().str.contains("Faculty"))
        .list.join(", ")
        .alias("institute"),
        pl.col("org_details_pp")
        .list.eval(pl.element().struct.field("faculty").struct.field("name"))
        .list.filter(pl.element().str.contains("Faculty"))
        .list.join(", ")
        .alias("faculty"),
        pl.col("org_details_pp")
        .list.eval(pl.element().struct.field("department").struct.field("name"))
        .list.filter(pl.element().is_not_null())
        .list.join(", ")
        .alias("department"),
        pl.col("org_details_pp")
        .list.eval(pl.element().struct.field("group").struct.field("name"))
        .list.filter(pl.element().is_not_null())
        .list.join(", ")
        .alias("group"),
        pl.col("org_details_pp")
        .list.eval(pl.element().struct.field("faculty").struct.field("abbr"))
        .list.filter(pl.element().is_not_null())
        .list.join(", ")
        .alias("faculty_abbr"),
        pl.col("org_details_pp")
        .list.eval(pl.element().struct.field("department").struct.field("abbr"))
        .list.filter(pl.element().is_not_null())
        .list.join(", ")
        .alias("department_abbr"),
        pl.col("org_details_pp")
        .list.eval(pl.element().struct.field("group").struct.field("abbr"))
        .list.filter(pl.element().is_not_null())
        .list.join(", ")
        .alias("group_abbr"),
    )

    return df


@app.function
@timing_decorator
def clean_publications(pure_publications: pl.DataFrame) -> pl.DataFrame:
    pure_publications = pure_publications.filter(pl.col("doi").is_not_null())

    pure_publications = (
        pure_publications.rename({"internal_repository_id": "pure_id"})
        .with_columns(
            pl.col("publication_date").replace(
                ["2-01-23", "3-03-15"], ["2023-01-02", "2015-03-03"]
            )
        )
        .with_columns(
            pl.col("doi")
            .str.to_lowercase()
            .replace("https://doi.org/", "")
            .str.strip_chars(),
            pl.coalesce([
                pl.col("publication_date").str.to_date("%Y-%m-%d", strict=False),
                pl.col("publication_date").str.to_date("%Y-%m", strict=False),
                pl.col("publication_date").str.to_date("%Y", strict=False),
            ]).alias("publication_date_cleaned"),
        )
        .with_columns(
            pl.col("publication_date_cleaned").dt.year().alias("publication_year")
        )
    )

    # drop unnecessary cols and fully null cols
    pure_publications = pure_publications.select([
        col
        for col in pure_publications.columns
        if pure_publications.select(pl.col(col).is_not_null().any()).to_series()[0]
    ])
    pure_publications = pure_publications[
        [
            s.name
            for s in pure_publications
            if not (s.null_count() == pure_publications.height)
        ]
    ]
    drop_cols = [
        "language",
        "number",
        "start_page",
        "edition",
        "issue",
        "isi",
        "endpage",
        "references",
    ]
    pure_publications = pure_publications.drop([
        x for x in drop_cols if x in pure_publications.columns
    ])

    # parse list cols that can also just be strs
    list_cols_to_reduce = ["title", "subtitle", "publishers", "license"]
    pure_publications = pure_publications.with_columns([
        pl.col(col).list.join("; ").alias(col)
        for col in list_cols_to_reduce
        if col in pure_publications.columns
    ])

    # do some fixing of data

    if "publishers" in pure_publications.columns:
        publisher_name_mapping = {
            "Elsevier": [
                "Elsevier B.V.",
                "Elsevier Ltd",
                "Elsevier Inc.",
                "Elsevier",
                "Elsevier Ireland Ltd",
                "Elsevier Doyma",
                "Elsevier Masson s.r.l.",
                "Elsevier Editora Ltda",
                "Elsevier (Singapore) Pte Ltd",
                "Elsevier USA",
                "Elsevier Bedrijfsinformatie",
            ],
            "Springer": [
                "Springer Science and Business Media B.V.",
                "Springer",
                "SpringerOpen",
                "Springer Science and Business Media, LLC",
                "Springer Science + Business Media",
                "Springer Spektrum",
            ],
            "Wiley": ["Wiley-Hindawi", "Wiley-VCH Verlag", "Wiley", "Wiley-Blackwell"],
            "Taylor & Francis": [
                "CRC Press (Taylor & Francis)",
                "Taylor and Francis Inc.",
                "Taylor and Francis A.S.",
                "Taylor & Francis Group LLC",
                "Taylor & Francis",
            ],
            "Nature": [
                "Nature Publishing Group",
                "Nature Partner Journals",
                "Nature Research",
            ],
            "IEEE": ["IEEE Advancing Technology for Humanity", "IEEE"],
            "UTwente": [
                "University of Twente/Julius-Maximilians-Universität Würzburg",
                "University of Twente, Faculty of Geo-Information Science and Earth Observation (ITC)",
                "University of Twente",
            ],
            "ACM": [
                "Association for Computing Machinery",
                "Association for Computing Machinery (ACM)",
                "ACM Publishing",
                "ACM SIGIR Forum",
                "ACM SigCHI",
                "ACM SIGCOMM",
                "ACM Press",
            ],
            "Frontiers": ["Frontiers Media SA", "Frontiers Research Foundation"],
            "Wolters Kluwer": [
                "Wolters Kluwer Medknow Publications",
                "Wolters Kluwer Health Inc",
                "Wolters Kluwer Health",
            ],
            "Sage": ["Sage Periodicals Press", "Sage"],
            "Inderscience": ["Inderscience", "Inderscience Publishers"],
        }

        # flip the mapping
        actual_mapping = {
            v: k for k, vals in publisher_name_mapping.items() for v in vals
        }
        pure_publications = pure_publications.with_columns(
            pl.col("publishers").replace(actual_mapping)
        )

    # now for some struct/list[struct] cols:

    # issn
    if "issn" in pure_publications.columns:
        issn_list = pure_publications[["pure_id", "issn"]].to_dicts()
        issn_list = [x for x in issn_list if x["issn"]]
        flat_list = []
        for item in issn_list:
            issns = set()
            for entry in item["issn"]:
                if entry.get("Print"):
                    issns.add(entry["Print"])
                if entry.get("Online"):
                    issns.add(entry["Online"])
            if not issns:
                continue
            flat_list.append({"pure_id": item["pure_id"], "issns": list(issns)})

        issn_df = pl.from_dicts(flat_list)
        pure_publications = pure_publications.join(
            issn_df, on="pure_id", how="left"
        ).drop("issn")

    # isbn
    # list of structs. from each struct, take value from field 'value'
    # store resulting list as list[str] in new col 'isbns'

    # store as new col 'isbns'
    if "isbn" in pure_publications.columns:
        pure_publications = pure_publications.with_columns(
            pl.col("isbn")
            .list.eval(pl.element().struct.field("value"))
            .list.drop_nulls()
            .list.unique()
            .alias("isbns")
        ).drop("isbn")

    # part_of
    if "part_of" in pure_publications.columns:
        pure_publications = (
            pure_publications.with_columns(
                pl.col("part_of").struct.field("cerif:Publication").alias("part_of")
            )
            .with_columns(pl.col("part_of").struct.unnest())
            .rename({
                "cerif:Title": "journal_name",
                "cerif:Subtitle": "journal_extra",
            })
            .with_columns(
                pl.col("journal_name").struct.field("#text").alias("source_title"),
                pl.col("journal_extra").struct.field("#text").alias("source_subtitle"),
            )
            .drop(["part_of", "journal_extra", "journal_name", "pubt:Type"])
        )

    return pure_publications


@app.function
@timing_decorator
def join_authors_and_publications(
    authors_df: pl.DataFrame, publications_df: pl.DataFrame
) -> pl.DataFrame:
    """
    Join authors and publications data to add crucial data like faculty/instititute/group affiliations.
    Input should be the output of 'clean_and_enrich_persons_data'+'enrich_employee_data'; and the 'clean_publications' functions.

    Returns:
        pl.DataFrame: Publications DataFrame enriched with merged author affiliation data -- so that for each publication we know which faculties/institutes any of the authors are affiliated with.
    """

    if (
        "internal_repository_id" not in authors_df.columns
        and "pure_id" not in authors_df.columns
    ):
        print(f"Found columns: {authors_df.columns}")
        raise ValueError(
            "authors_df must contain either a 'internal_repository_id' or 'pure_id' column."
        )
    if "pure_id" not in authors_df.columns:
        authors_df = authors_df.rename({"internal_repository_id": "pure_id"})

    pubs_with_author_ids = publications_df.with_columns(
        pl.col("authors")
        .list.eval(pl.element().struct.field("internal_repository_id"))
        .list.drop_nulls()
        .alias("author_pure_ids")  # Give it a distinct name to avoid confusion.
    )
    exploded_pubs = pubs_with_author_ids.select(["pure_id", "author_pure_ids"]).explode(
        "author_pure_ids"
    )

    author_details = exploded_pubs.join(
        authors_df, left_on="author_pure_ids", right_on="pure_id", how="left"
    )
    # group data by 'pure_id', adding col 'all_names' (list[str]) with all the values in 'first_name' for any row with that pure_id

    # set up aggregration

    # cols to aggregrate
    merge_cols_bool = ["dsi", "mesa", "techmed", "eemcs", "et", "bms", "tnw", "itc"]
    merge_cols_lists = [
        "faculty",
        "institute",
        "department",
        "group",
        "faculty_abbr",
        "department_abbr",
        "group_abbr",
    ]
    merge_cols_str = ["orcid"]

    # bools: check if 'any' author has a True value.
    agg_exprs = [
        pl.col(col).any().alias(col)
        for col in merge_cols_bool
        if col in author_details.columns
    ]

    # list[str]s: flatten all lists into one, then get unique values.
    agg_exprs.extend([
        pl.col(col)
        .str.split(by="; ")
        .flatten()
        .unique()
        .replace("", None)
        .drop_nulls()
        .alias(col)
        for col in merge_cols_lists
        if col in author_details.columns
    ])

    # strs: collect all non-null, unique strings into a new list.
    agg_exprs.extend([
        pl.col(col).drop_nulls().unique().alias(col + "s")
        for col in merge_cols_str
        if col in author_details.columns
    ])

    # group and aggregrate
    merged_author_data = author_details.group_by("pure_id").agg(agg_exprs)

    # join and return
    final_df = publications_df.join(merged_author_data, on="pure_id", how="left")

    return final_df


@app.function
@timing_decorator
def get_counts_per_pure_publisher(
    enriched_publications_df: pl.DataFrame,
    faculty_list: list[str] | None,
    institute_list: list[str] | None,
) -> pl.DataFrame:
    """
    For a given df with enriched publication data, calculate counts of items per publisher per faculty/institute.
    You can specify a list of faculties/institutes to include; if None, defaults will be used (all core faculties and all institutes)
    It does the following:

     for each publisher in 'publishers' ('str' col, with max one publisher name per cell):
     - count amount of rows with that publisher in ut_publications_enriched
     - then also make counts per faculty/institute:
     grab the bool cols (tnw, eemcs, bms, et, itc, dsi, techmed, mesa) per publisher and calculate the sum of each
     also count the sum of all core faculties (tnw, eemcs, bms, et, itc) as 'total_faculty_count'
    """
    faculty_list = faculty_list or ["tnw", "bms", "et", "itc", "eemcs"]
    institute_list = institute_list or ["dsi", "techmed", "mesa"]
    facs_and_insts = faculty_list + institute_list
    faculty_list = [x for x in faculty_list if x in enriched_publications_df.columns]
    facs_and_insts = [
        x for x in facs_and_insts if x in enriched_publications_df.columns
    ]
    final_select = ["publisher", "total"] + [x + "_count" for x in facs_and_insts]

    if faculty_list:
        maybe_agg = pl.sum_horizontal([x + "_count" for x in faculty_list]).alias(
            "faculty_sum"
        )
        final_select.append("faculty_sum")
    else:
        maybe_agg = []

    return (
        enriched_publications_df.filter(pl.col("publishers").is_not_null())
        .group_by("publishers")
        .agg(
            [pl.col("publishers").count().alias("total")]
            + [pl.col(col).sum().alias(col + "_count") for col in facs_and_insts]
        )
        .with_columns(maybe_agg)
        .rename({"publishers": "publisher"})
        .select(final_select)
        .sort("total", descending=True)
    )


@app.function
@timing_decorator
def get_openalex_works_bulk_by_id(
    ids: list[str], id_type: str = "doi", clean_data: bool = True, per_page: int = 50
) -> pl.DataFrame:
    """
    for a given list of DOIs or openalex ids, retrieve the corresponding works from OpenAlex API. Set the type of id with id_type ('doi' or 'id').
    if clean_data is True, process the raw data to extract relevant fields and return a cleaned DataFrame.

    """
    openalex_works_api_url = "https://api.openalex.org/works"
    headers = {"User-Agent": "mailto:s.mok@utwente.nl"}
    if id_type == "id":
        id_type = "openalex"
    if id_type not in ["doi", "openalex"]:
        raise ValueError(f"id_type must be either 'doi' or 'id', not: {id_type}")
    ids_chunks = [ids[i : i + 50] for i in range(0, len(ids), 50)]
    all_works = []
    print(f"retrieving {len(ids)} items from OpenAlex using {id_type} identifiers")
    with httpx.Client(headers=headers) as client:
        for chunk in mo.status.progress_bar(
            collection=ids_chunks,
            show_eta=True,
            show_rate=True,
            title="Retrieving works from OpenAlex",
        ):
            filter_query = "|".join([str(x).replace("doi: ", "") for x in chunk])
            params = {"filter": f"{id_type}:{filter_query}", "per-page": per_page}
            response = client.get(openalex_works_api_url, params=params)
            try:
                response.raise_for_status()
                data = response.json()
            except Exception:
                # wait 4 seconds and retry
                sleep(4)
                response = client.get(openalex_works_api_url, params=params)
                try:
                    response.raise_for_status()
                    data = response.json()
                except Exception:
                    # reduce amount of results per page, split into multiple queries, combine results
                    temp_per_page = 5
                    temp_works = []
                    for start in range(0, len(chunk), temp_per_page):
                        try:
                            sub_chunk = chunk[start : start + temp_per_page]
                            sub_filter_query = "|".join([
                                str(x).replace("doi: ", "") for x in sub_chunk
                            ])
                            sub_params = {
                                "filter": f"{id_type}:{sub_filter_query}",
                                "per-page": temp_per_page,
                            }
                            sub_response = client.get(
                                openalex_works_api_url, params=sub_params
                            )
                            sub_response.raise_for_status()
                            sub_data = sub_response.json()
                            all_works.extend(sub_data["results"])
                            sleep(1)
                        except Exception as e:
                            print(
                                f"error while trying to retrieve data from OA api for sub-chunk with ids {sub_filter_query}: {e}. Skipping."
                            )
                            continue
                    continue

            all_works.extend(data["results"])
            print(f"retrieved {len(all_works)} items. (+{len(data['results'])})")

    results = pl.from_dicts(all_works)
    return clean_openalex_raw_data(results) if clean_data else results


@app.cell
def _(verbose):
    @timing_decorator
    def get_openalex_works_by_title(
        titles: list[str], retry_unfound: bool = False
    ) -> list[dict[str, str]]:
        """
        for a given list of titles, use the autocomplete api to find the corresponding OpenAlex works.
        """
        autocomplete_url = "https://api.openalex.org/autocomplete/works"
        headers = {"User-Agent": "mailto:s.mok@utwente.nl"}

        # use the autocomplete endpoint to get ids, collate them, grab the details in a second pass
        # https://api.openalex.org/autocomplete?q={title}
        # if that doesnt work we can  use title.search; probably with .no_stem, eg
        # https://api.openalex.org/works?filter=display_name.search.no_stem:surgery

        results = []
        not_found = []
        print(f"retrieving {len(titles)} items")
        with httpx.Client(headers=headers) as client:
            for title in mo.status.progress_bar(
                collection=titles,
                show_eta=True,
                show_rate=True,
                title="Retrieving ids from OpenAlex by title",
            ):
                params = {"q": title}
                try:
                    response = client.get(autocomplete_url, params=params)

                    if response.status_code == 429:
                        sleep(2)
                        response = client.get(autocomplete_url, params=params)
                except Exception as e:
                    print(
                        f"error while trying to retrieve data from OA api for title '{title}': {e}. Skipping."
                    )
                    continue

                try:
                    response.raise_for_status()
                    data = response.json()
                    if len(data["results"]) == 0:
                        not_found.append(title)
                        continue
                    item = data["results"][0]
                    results.append({
                        "search_title": title,
                        "found_title": item.get("display_name"),
                        "id": item.get("id"),
                        "doi": item.get("external_id"),
                        "authors": item.get("hint"),
                    })
                    if verbose:
                        print(
                            f"[{item.get('id')}] input:\n{title}\n         found:\n{item.get('display_name')}\n---------------\n"
                        )
                except Exception as e:
                    print(
                        f"error while trying to retrieve data from OA api for title '{title}': {e}. Skipping."
                    )
                    continue

        print(
            f"retrieved {len(results)} items from {len(titles)} input titles. {len(not_found)} titles without results."
        )
        if retry_unfound:
            # for each title, cut off subtitles and retry
            print("retrying unfound titles by stripping subtitles...")
            stripped_titles = [t.split(":")[0].strip() for t in not_found]
            more_results = get_openalex_works_by_title(
                stripped_titles, retry_unfound=False
            )
            if more_results:
                results.extend(more_results)

        return results

    return (get_openalex_works_by_title,)


@app.function
@timing_decorator
def clean_openalex_raw_data(df: pl.DataFrame) -> pl.DataFrame:
    """
    for a given df with raw OpenAlex works data, extract relevant fields and drop large/not interesting cols.
    """

    dropcols = [
        "abstract_inverted_index",
        "related_works",
        "has_content",
        "awards",
        "concepts",
        "keywords",
        "is_xpac",
        "is_retracted",
        "is_paratext",
        "updated_date",
        "created_date",
    ]

    utwente_oa_id = "https://openalex.org/I94624287"

    return df.with_columns(
        pl.col("open_access").struct.field("is_oa").alias("is_oa"),
        pl.col("open_access").struct.field("oa_status").alias("oa_color"),
        pl.col("open_access")
        .struct.field("any_repository_has_fulltext")
        .alias("in_repository"),
        pl.col("open_access").struct.field("oa_url").alias("oa_url"),
        pl.col("best_oa_location").struct.field("landing_page_url").alias("main_url"),
        pl.col("best_oa_location")
        .struct["source"]
        .struct["host_organization_name"]
        .alias("oa_host_org"),
        pl.col("best_oa_location")
        .struct.field("source")
        .struct.field("display_name")
        .alias("oa_host_name"),
        pl.col("best_oa_location")
        .struct.field("source")
        .struct.field("type")
        .alias("oa_host_type"),
        pl.col("primary_location")
        .struct.field("landing_page_url")
        .alias("primary_url"),
        pl.col("primary_location")
        .struct["source"]
        .struct["host_organization_name"]
        .alias("primary_host_org"),
        pl.col("primary_location")
        .struct.field("source")
        .struct.field("display_name")
        .alias("primary_host_name"),
        pl.col("primary_location")
        .struct.field("source")
        .struct.field("type")
        .alias("primary_host_type"),
        pl.col("locations")
        .list.eval(
            pl.element()
            .struct.field("source")
            .struct.field("host_organization_name")
            .unique()
            .drop_nulls()
        )
        .alias("all_host_orgs"),
        pl.col("primary_topic").struct.field("display_name").alias("topic"),
        pl.col("primary_topic")
        .struct.field("subfield")
        .struct.field("display_name")
        .alias("subfield"),
        pl.col("primary_topic")
        .struct.field("field")
        .struct.field("display_name")
        .alias("field"),
        pl.col("primary_topic")
        .struct.field("domain")
        .struct.field("display_name")
        .alias("domain"),
        pl.col("apc_list").struct.field("value_usd").alias("listed_apc_usd"),
        pl.col("apc_paid").struct.field("value_usd").alias("paid_apc_usd"),
        pl.col("corresponding_institution_ids")
        .list.contains(utwente_oa_id)
        .alias("ut_is_corresponding"),
    ).drop([x for x in dropcols if x in df.columns])


@app.cell
def _(get_openalex_works_by_title):
    @timing_decorator
    def add_missing_items_by_title(df: pl.DataFrame):
        unfound_items = (
            df.filter(pl.col("id").is_null())
            .with_columns(pl.col("title").str.strip_chars().alias("clean_title"))
            .drop_nulls("clean_title")
            .with_columns(
                pl.when(
                    (pl.col("subtitle").is_not_null()) & (pl.col("subtitle").ne(""))
                )
                .then(pl.col("clean_title") + pl.lit(": ") + pl.col("subtitle"))
                .otherwise(pl.col("clean_title"))
                .alias("search_title")
            )
            .select(["title", "subtitle", "clean_title", "search_title"])
        )

        print(
            f"{unfound_items.height} items missing OpenAlex data: searching by title."
        )

        work_data_by_title = get_openalex_works_by_title(
            unfound_items.select("search_title").unique().to_series().to_list(),
            retry_unfound=True,
        )

        more_works = get_openalex_works_bulk_by_id(
            [x.get("id") for x in work_data_by_title if x.get("id")],
            id_type="id",
            clean_data=True,
        )

        # determine how many items in 'more_works' are not in 'df_with_more_works'
        # then merge by title for remaining unmerged items
        # add original search string

        unmerged = (
            more_works.with_columns(
                pl.col("id")
                .replace({
                    x.get("id"): x.get("search_title")
                    for x in work_data_by_title
                    if x.get("id")
                })
                .alias("search_title")
            )
            .with_columns(
                pl.col("search_title")
                .str.split(":")
                .list.first()
                .alias("search_title_nosub")
            )
            .unique("id")
        )

        print(f"{unmerged.height} items to merge")
        print(
            f"{df.height} items in main df, {df.filter(pl.col('id').is_null()).height} have no openalexid"
        )

        ratio_df = pl.DataFrame([
            {
                "id": x["id"],
                "title_ratio": difflib.SequenceMatcher(
                    None,
                    x["display_name"].lower().strip(),
                    x["search_title"].lower().strip(),
                    autojunk=True,
                ).ratio(),
            }
            for x in unmerged.select(["id", "display_name", "search_title"]).to_dicts()
        ])
        unmerged = unmerged.join(ratio_df, how="left", on="id")

        unmerged = unmerged.with_columns(
            pl.col("institutions")
            .list.eval(pl.element().struct.field("display_name"))
            .alias("inst_names")
        ).with_columns(
            pl.col("inst_names")
            .list.contains("University of Twente")
            .alias("ut_found_in_oa")
        )

        print(
            f"of the {unmerged.height} unmerged items, {unmerged.filter(pl.col('title_ratio') > 0.9).height} have title similarity > 0.9, and {unmerged.filter(pl.col('ut_found_in_oa')).height} have UT in their affiliations."
        )

        with_more_works = df.join(
            unmerged,
            left_on="title",
            right_on="search_title",
            how="left",
            suffix="_oa_extra",
        )
        not_yet_found = unmerged.filter(
            ~pl.col("id").is_in(
                with_more_works.select("id_oa_extra").to_series().to_list()
            )
        )
        print(f"{not_yet_found.height} unmerged left after merge 1")

        with_more_works = with_more_works.join(
            not_yet_found,
            left_on="title",
            right_on="search_title_nosub",
            how="left",
            suffix="_oa_extra_2",
        )

        not_yet_found = not_yet_found.filter(
            ~pl.col("id").is_in(
                with_more_works.select("id_oa_extra_2").to_series().to_list()
            )
        )
        print(f"{not_yet_found.height} unmerged left after merge 2")

        # some manual fixed title matches
        match_titles = pl.DataFrame([
            {
                "id": "https://openalex.org/W2753835807",
                "match_title": "Geographic variability of Twitter usage characteristics during disaster events : open access",
            },
            {
                "id": "https://openalex.org/W4410616102",
                "match_title": "On the thermal degradation of lubricant grease: Degradation analysis",
            },
            {
                "id": "https://openalex.org/W576524927",
                "match_title": "High-impact low-probability events: Exposure to potential large-magnitude explosive volcanic eruptions",
            },
            {
                "id": "https://openalex.org/W2767096043",
                "match_title": "Peer Review: Poland's Higher Education and Science System",
            },
            {
                "id": "https://openalex.org/W2348907925",
                "match_title": "In Conclusion: Doing more with Less",
            },
            {
                "id": "https://openalex.org/W2506242984",
                "match_title": "Beyond Movie Recommendations: Solving the Continuous Cold Start Problem in E-commerce Recommendations",
            },
            {
                "id": "https://openalex.org/W2904520019",
                "match_title": "CAPICE: childhood and adolescence psychopathology",
            },
            {
                "id": "https://openalex.org/W2915822876",
                "match_title": "Weighted Quasi Akash Distribution:",
            },
            {
                "id": "https://openalex.org/W2971570088",
                "match_title": "Social Inclusion Policies in Higher Education: Evidence from the EU",
            },
        ])

        not_yet_found = not_yet_found.join(
            match_titles, left_on="id", right_on="id", how="left"
        )

        with_more_works = with_more_works.join(
            not_yet_found,
            left_on="title",
            right_on="match_title",
            how="left",
            suffix="_oa_extra_3",
        )

        not_yet_found = not_yet_found.filter(
            ~pl.col("id").is_in(
                with_more_works.select("id_oa_extra_3").to_series().to_list()
            )
        )

        print(f"{not_yet_found.height} unmerged left after merge 3")

        # now we coalesce the columns from the merging
        # if all went well, all _oa_extra_n cols should be mergable into empty cols without the suffix
        with_more_works = with_more_works.drop([
            x
            for x in with_more_works.columns
            if any([
                "ratio" in x,
                "inst_names" in x,
                "search_title" in x,
                "search_title_nosub" in x,
                "match_title" in x,
                "ut_found_in_oa" in x,
            ])
        ]).with_columns([
            pl.col(col).list.eval(pl.element().struct.field("display_name")).alias(col)
            for col in [
                "funders_oa",
                "funders_oa_extra",
                "funders_oa_extra_2",
                "funders_oa_extra_3",
            ]
            if col in with_more_works.columns
        ])

        suffix_cols = [
            col
            for col in with_more_works.columns
            if col.endswith("_oa_extra")
            or col.endswith("_oa_extra_2")
            or col.endswith("_oa_extra_3")
        ]

        base_col_names = [
            list(
                set([
                    col.rsplit("_oa_extra", 1)[0]
                    .rsplit("_oa_extra_2", 1)[0]
                    .rsplit("_oa_extra_3", 1)[0]
                    for col in suffix_cols
                ])
            )
        ]
        base_col_names = [
            col
            for col in base_col_names[0]
            if any(
                (col + suffix) in with_more_works.columns
                for suffix in ["_oa_extra", "_oa_extra_2", "_oa_extra_3"]
            )
        ]

        col_groups = [
            [
                base_col,
                base_col + "_oa_extra",
                base_col + "_oa_extra_2",
                base_col + "_oa_extra_3",
            ]
            for base_col in base_col_names
        ]

        final_df = (
            with_more_works.with_columns(
                # replace empty strings with nulls
                [
                    pl.when(pl.col(col).eq(""))
                    .then(pl.lit(None))
                    .otherwise(pl.col(col))
                    .alias(col)
                    for col in with_more_works.columns
                    if with_more_works[col].dtype == pl.Utf8
                ]
            )
            .with_columns([
                pl.coalesce(*col_group).alias(col_group[0]) for col_group in col_groups
            ])
            .drop(suffix_cols)
        )

        return final_df

    return (add_missing_items_by_title,)


@app.cell
def _(add_missing_items_by_title):
    @timing_decorator
    def add_openalex_to_df(
        df: pl.DataFrame,
        doi_col: str = "doi",
        use_titles: bool = False,
        title_col: str = "title",
    ) -> pl.DataFrame:
        """
        For a given df with a column with dois, retrieve corresponding OpenAlex works and merge them into the df.

        if 'use_titles' is True, will also run a second pass for items that were not found by DOI, using title search.
        """
        df = df.with_columns(
            pl.col(doi_col).str.to_lowercase().str.strip_chars().alias("clean_doi")
        )
        all_dois = df.select("clean_doi").unique().to_series().to_list()
        openalex_works = get_openalex_works_bulk_by_id(all_dois, "doi")
        merged_df = df.join(
            openalex_works.with_columns(
                pl.col("doi")
                .str.replace("https://doi.org/", "")
                .str.to_lowercase()
                .str.strip_chars()
                .alias("clean_doi")
            ),
            on="clean_doi",
            how="left",
            suffix="_oa",
        )

        # if 'use_titles' == True: retry non-matches using title search
        if not use_titles:
            return merged_df
        return add_missing_items_by_title(merged_df)

    return (add_openalex_to_df,)


@app.cell
def _():
    import polars.selectors as cs

    @timing_decorator
    def export_data(
        merged_df: pl.DataFrame,
        excel_path: str = None,
        parquet_path: str = None,
    ) -> pl.DataFrame:
        """
        Prepares dataframe for excel output: removed unwanted/unnecessary columns and sorts cols.
        Stores the resulting dataframe both as excel and parquet file, and returns the df as well.
        """
        # set these cols at the front of output in this order
        first_cols = [
            "doi_url",
            "id",
            "deal_oils",
            "listed_apc_usd",
            "paid_apc_usd",
            "ut_is_corresponding",
            "oa_color",
            "license",
            "publishers",
            "primary_host_org",
            "all_host_orgs",
            "publisher_oils",
            "oils_match",
            "openalex_match",
            "pure_match",
            "publication_year_oa",
            "year_oils",
            "publication_year",
            "faculty_abbr",
        ]
        # drop these cols from output
        drop_cols = [
            "authorships",
            "authors",
            "locations",
            "sustainable_development_goals",
            "referenced_works",
            "counts_by_year",
            "locations_count",
            "topics",
            "primary_topic",
            "cited_by_percentile_year",
            "citation_normalized_percentile",
            "apc_paid",
            "apc_list",
            "corresponding_institution_ids",
            "corresponding_author_ids",
            "indexed_in",
            "language",
            "referenced_works_count",
            "grants",
        ]

        all_cols = list(merged_df.columns)
        all_cols = [
            x for x in all_cols if (x not in drop_cols) and (x not in first_cols)
        ]
        all_cols = first_cols + all_cols
        all_cols = [col for col in all_cols if col in merged_df.columns]
        merged_df = merged_df.select(all_cols)
        if parquet_path:
            merged_df.write_parquet(parquet_path)
        if excel_path:
            merged_df.with_columns(
                pl.col("id").str.replace("https://", ""),
                pl.col("doi").str.replace("https://", ""),
                pl.col("doi_url").str.replace("https://", ""),
                pl.col("url").str.replace("https://", ""),
            ).write_excel(excel_path, column_formats={~cs.temporal(): "General"})
        return merged_df


@app.function
@timing_decorator
def filter_publisher(df: pl.DataFrame, publisher_filter: list[str]):
    """
    filters a df based on a list of publisher names.
    """

    print(f"started with {df.height} rows")
    publisher_cols = ["publishers", "primary_host_org", "all_host_orgs"]
    selected_publisher_cols = [x for x in df.columns if x in publisher_cols]
    if len(selected_publisher_cols) == 0:
        print("no publisher columns found in dataframe")
        return df
    if len(selected_publisher_cols) == 1:
        col_name = selected_publisher_cols[0]
        if col_name == "all_host_orgs":
            filtered_df = df.filter(pl.col(col_name).list.contains(publisher_filter[0]))
        else:
            filtered_df = df.filter(pl.col(col_name).is_in(publisher_filter))
    else:
        filtered_df = df.filter(
            pl.col("publishers").is_in(publisher_filter)
            | pl.col("primary_host_org").is_in(publisher_filter)
            | pl.col("all_host_orgs").list.contains(publisher_filter[0])
        )

    print(f"finished with {filtered_df.height} rows")
    return filtered_df


@app.function
@timing_decorator
def merge_oils_with_all(oils_df: pl.DataFrame, full_df: pl.DataFrame) -> pl.DataFrame:
    rename_dict = {
        "Title_1": "Journal",
        "Keywords (free keywords)": "Keywords",
        "Pure ID": "PureID",
        "DOI": "doi",
    }
    rename_dict = {k: v for k, v in rename_dict.items() if k in oils_df.columns}

    oils_df = oils_df.rename(rename_dict)
    oils_df = oils_df.rename({
        col: (col + "_oils").lower().replace(" ", "_") for col in oils_df.columns
    })

    merged_df = full_df.join(
        oils_df,
        left_on="doi",
        right_on="doi_oils",
        how="full",
    )
    match_expressions = []
    if "id" in merged_df.columns:
        match_expressions.append(
            pl.when(pl.col("id").is_not_null())
            .then(pl.lit(True))
            .otherwise(pl.lit(False))
            .alias("openalex_match")
        )
    if "pureid_oils" in merged_df.columns:
        match_expressions.append(
            pl.when(pl.col("pureid_oils").is_not_null())
            .then(pl.lit(True))
            .otherwise(pl.lit(False))
            .alias("oils_match")
        )
    if "pure_id" in merged_df.columns:
        match_expressions.append(
            pl.when(pl.col("pure_id").is_not_null())
            .then(pl.lit(True))
            .otherwise(pl.lit(False))
            .alias("pure_match")
        )

    merged_df = merged_df.with_columns(*match_expressions)
    return merged_df


@app.function
@timing_decorator
def extract_author_and_funder_names(df: pl.DataFrame) -> pl.DataFrame:
    extractions = []
    if "authorships" in df.columns:
        if df["authorships"].dtype == pl.Struct:
            extractions.extend([
                pl.col("authorships")
                .list.eval(
                    pl.element()
                    .struct.field("author")
                    .struct.field("display_name")
                    .drop_nulls()
                )
                .alias("oa_authors_names"),
                pl.col("authorships")
                .list.eval(
                    pl.element()
                    .struct.field("author")
                    .struct.field("orcid")
                    .drop_nulls()
                )
                .alias("oa_authors_orcids"),
            ])
    if "funders" in df.columns:
        if df["funders"].dtype == pl.Struct:
            extractions.append(
                pl.col("funders")
                .list.eval(pl.element().struct.field("display_name").drop_nulls())
                .alias("funders"),
            )
    if "authors" in df.columns:
        if df["authors"].dtype == pl.Struct:
            extractions.append(
                pl.col("authors")
                .list.eval(
                    pl.concat_str(
                        [
                            pl.element().struct.field("first_names"),
                            pl.element().struct.field("family_names"),
                        ],
                        separator=" ",
                    ).drop_nulls()
                )
                .alias("pure_authors_names")
            )

    return df.with_columns(*extractions)


@app.function
@timing_decorator
def add_missing_affils(
    df: pl.DataFrame, more_data: list[dict[str, list[str]]] | None = None
) -> pl.DataFrame:
    """
    a hacky quick fix for adding missing affiliation data for certain authors.
    """
    # for each person in more_data
    # key=name, value = list of abbrs
    # parse the abbreviations:
    # - split each by "-" to extract faculty abbr, department abbr, group abbr
    # - if no '-': institute only

    # then set the abbr fields + bool cols accordingly for that row

    # we end up with a df with the fields to be updated and a name to match on

    # then, for each row in 'df', match the name in list[str] col 'pure_authors_names'
    # if there is a match, update the fields accordingly:
    # - for the abbrs, add to the list[str] col if not already present
    # - for the bools, do a logical 'or' with the existing value (i.e. if either is True, set to True, else False)

    more_author_data = [
        {"Ioannis Sechopoulos": ["TNW-BIS-M3I"]},
        {"Luca Mariot": ["EEMCS-CS-SCS"]},
        {"Yang Miao": ["EEMCS-EE-RS", "dsi"]},
        {"Carolien Rieffe": ["EEMCS-CS-HMI", "dsi"]},
        {"Cornelis H. Venner": ["ET-TFE-EFD"]},
        {"Frank Leferink": ["EEMCS-EE-RS", "EEMCS-EE-PE", "dsi"]},
        {"Nico Verdonschot": ["ET-BE-BDDP", "techmed"]},
        {"Rolands Kromanis": ["ET-CEM-MD"]},
        {"Laura Botero-Bolívar": ["ET-TFE-EFD"]},
        {"Ian Gibson": ["ET-DPM-AMSPES"]},
        {"Payam Kaghazchi": ["TNW-NEM-IMS"]},
        {"Arnd Hartmanns": ["EEMCS-CS-FMT"]},
        {"Hans Voordijk": ["ET-CEM-MD"]},
        {"Gozewijn D. Laverman": ["EEMCS-EE-BSS", "techmed"]},
        {"Chris L. de Korte": ["TNW-POF-POF"]},
        {"Amirreza Yousefzadeh": ["EEMCS-CS-CAES"]},
        {"Ernst Moritz Hahn": ["EEMCS-CS-FMT", "dsi"]},
        {"Ana Lucia Varbanescu": ["EEMCS-CS-CAES"]},
        {"Hans Zwart": ["EEMCS-AM-MAST", "dsi"]},
        {"Wim H. van Harten": ["BMS-TPS-HTSR"]},
        {"Maximilian A. Friehs": ["BMS-HIB-PCRS"]},
        {"Can Ozan Tan": ["EEMCS-EE-RAM"]},
        {"Alessandro Chiumento": ["EEMCS-CS-PS", "dsi"]},
        {"Antonios Antoniadis": ["EEMCS-AM-MOR"]},
        {"Walter van der Meer": ["TNW-MST-MSUS"]},
        {"A. Veldkamp": ['ITC-EOS-""']},
    ]
    more_data = more_data or more_author_data
    updates = []
    bool_cols = ["tnw", "eemcs", "et", "bms", "itc", "dsi", "techmed", "mesa"]
    list_cols = ["faculty_abbr", "department_abbr", "group_abbr", "institute"]

    for data in more_data:
        update_dict = dict.fromkeys(bool_cols, False)
        update_dict.update(dict.fromkeys(list_cols))
        update_dict["name"] = list(data.keys())[0]
        affils = data[update_dict["name"]]
        for abbr in affils:
            if "-" in abbr:
                (
                    update_dict["faculty_abbr"],
                    update_dict["department_abbr"],
                    update_dict["group_abbr"],
                ) = str(abbr).split("-")
                update_dict[update_dict["faculty_abbr"].lower()] = True
            else:
                update_dict["institute"] = abbr
                update_dict[abbr] = True
        updates.append(update_dict)

    updates_df = pl.from_dicts(updates)

    df = df.explode("pure_authors_names").join(
        updates_df,
        left_on="pure_authors_names",
        right_on="name",
        how="left",
        suffix="_upd",
    )
    # append new unique values to list[str] cols
    df = df.with_columns([
        pl.when(pl.col(col + "_upd").is_not_null())
        .then(pl.concat_list([pl.col(col), pl.col(col + "_upd")]).list.unique())
        .otherwise(pl.col(col))
        .alias(col)
        for col in list_cols
        if col + "_upd" in df.columns
    ]).drop([name + "_upd" for name in list_cols if name + "_upd" in df.columns])

    # logical 'or' for bool cols
    bool_cols = ["tnw", "eemcs", "et", "bms", "itc", "dsi", "techmed", "mesa"]
    df = df.with_columns([
        pl.when(pl.col(col + "_upd").is_not_null())
        .then(pl.col(col).or_(pl.col(col + "_upd")))
        .otherwise(pl.col(col))
        .alias(col)
        for col in bool_cols
        if col + "_upd" in df.columns
    ]).drop([name + "_upd" for name in bool_cols if name + "_upd" in df.columns])

    # now undo the explode to restore original structure
    df = df.group_by([
        col for col in df.columns if col not in ["pure_authors_names", "name"]
    ]).agg(pl.col("pure_authors_names"))
    return df


@app.cell
def _(all_plus_oa_elsevier, faculty_cols, oils_plus_oa):
    def create_charts():
        datasets = {
            "UT Elsevier publications 2022-2024 - Mok - 2018 publications": all_plus_oa_elsevier,
            "UT Elsevier publications 2022-2024 -  OILS - 1551 publications": oils_plus_oa,
        }
        chart_rows = []

        for name, df in datasets.items():
            yearly_counts = (
                df.group_by("publication_year")
                .agg([pl.col(fac).sum() for fac in faculty_cols])
                .melt(
                    id_vars=["publication_year"],
                    value_vars=faculty_cols,
                    variable_name="faculty",
                    value_name="publication_count",
                )
                .sort("publication_year")
            )

            total_counts = (
                df.select([pl.col(fac).sum() for fac in faculty_cols])
                .melt(variable_name="faculty", value_name="publication_count")
                .sort("publication_count", descending=True)
            )

            total_labels_df = (
                yearly_counts.group_by("publication_year")
                .agg(
                    pl.col("publication_count").sum().alias("total_count"),
                    pl.col("publication_count").max().alias("max_bar_height"),
                )
                .with_columns(
                    pl.format(
                        "{}\n{} items total",
                        pl.col("publication_year"),
                        pl.col("total_count"),
                    ).alias("label_text")
                )
            )

            faculty_cols_overlap = [x for x in faculty_cols if x != "institute"]

            overlap_counts = (
                df.with_columns(
                    pl.concat_str(
                        [
                            pl.when(pl.col(fac)).then(pl.lit(fac))
                            for fac in sorted(faculty_cols_overlap)
                        ],
                        separator="+",
                        ignore_nulls=True,
                    ).alias("faculty_combination")
                )
                .group_by("faculty_combination")
                .count()
                .filter(
                    pl.col("faculty_combination") != ""
                )  # Remove rows with no faculty
                .rename({"count": "publication_count"})
                .sort("publication_count", descending=True)
            )

            yearly_chart_title = f"{name}: Elsevier Publications per Faculty by Year"
            bar_chart = (
                alt.Chart(yearly_counts)
                .mark_bar()
                .encode(
                    x=alt.X(
                        "publication_year:O",
                        title="Publication Year",
                        axis=alt.Axis(labelAngle=0),
                    ),
                    y=alt.Y("publication_count:Q", title="Number of Publications"),
                    color=alt.Color("faculty:N", title="Faculty"),
                    xOffset=alt.XOffset("faculty:N"),
                )
            )
            text_labels = (
                alt.Chart(total_labels_df)
                .mark_text(
                    align="center",
                    baseline="bottom",
                    fontSize=14,
                    fontWeight="bold",
                    color="white",
                    lineBreak="\n",
                )
                .encode(
                    x="publication_year:O", y="max_bar_height:Q", text="label_text:N"
                )
            )
            final_yearly_chart = (bar_chart + text_labels).properties(
                title=yearly_chart_title, width=alt.Step(80)
            )

            total_chart_title = f"{name} - Overall Totals"
            total_bars = (
                alt.Chart(total_counts, width=500)
                .mark_bar()
                .encode(
                    x=alt.X("faculty:N", title="Faculty", sort="-y"),
                    y=alt.Y("publication_count:Q", title="Total Publications"),
                    color=alt.Color("faculty:N", title="Faculty"),
                )
            )
            total_bar_labels = total_bars.mark_text(
                align="center", baseline="bottom", color="white"
            ).encode(text="publication_count:Q")
            final_total_chart = (total_bars + total_bar_labels).properties(
                title=total_chart_title
            )

            overlap_chart_title = f"{name} - Faculty Combination Counts"

            overlap_bars = (
                alt.Chart(overlap_counts, width=700)
                .mark_bar()
                .encode(
                    x=alt.X(
                        "faculty_combination:N", title="Faculty Combination", sort="y"
                    ),
                    y=alt.Y("publication_count:Q", title="Number of Publications"),
                    color=alt.Color("faculty_combination:N", title="Faculty Combo"),
                )
            )

            overlap_bar_labels = overlap_bars.mark_text(
                align="center", baseline="bottom", color="white"
            ).encode(text="publication_count:Q")

            final_overlap_chart = (overlap_bars + overlap_bar_labels).properties(
                title=overlap_chart_title
            )

            chart_rows.append([
                mo.ui.altair_chart(final_yearly_chart),
                mo.ui.altair_chart(final_total_chart),
                mo.ui.altair_chart(final_overlap_chart),
            ])

        return chart_rows

    # create_charts()


@app.cell
def _(all_plus_oa_elsevier, oils_plus_oa):
    def oa_charts():
        datasets = {
            "Larger set (2018 total)": all_plus_oa_elsevier,
            "OILS dataset (1551 total)": oils_plus_oa,
        }
        chart_pairs = []

        for name, df in datasets.items():
            # --- Step 1: Data Preparation for BOTH charts ---

            # Data for the detailed yearly chart
            yearly_counts = (
                df.group_by("publication_year", "oa_color")
                .count()
                .rename({"count": "publication_count"})
                .filter(pl.col("oa_color").is_not_null())
                .sort("publication_year")
            )

            # Data for the high-level totals chart
            total_counts = (
                df.group_by("oa_color")
                .count()
                .rename({"count": "publication_count"})
                .filter(pl.col("oa_color").is_not_null())
                .sort("publication_count", descending=True)
            )

            # Data for the year-total text labels (as before)
            total_labels_df = (
                yearly_counts.group_by("publication_year")
                .agg(
                    pl.col("publication_count").sum().alias("total_count"),
                    pl.col("publication_count").max().alias("max_bar_height"),
                )
                .with_columns(
                    pl.format(
                        "{}\n{} items total",
                        pl.col("publication_year"),
                        pl.col("total_count"),
                    ).alias("label_text")
                )
            )

            # --- Step 2: Create the Yearly Chart (with annotations) ---

            yearly_chart_title = f"{name}: Elsevier Publications per OA Status by Year"

            bar_chart = (
                alt.Chart(yearly_counts)
                .mark_bar()
                .encode(
                    x=alt.X(
                        "publication_year:O",
                        title="Publication Year",
                        axis=alt.Axis(labelAngle=0),
                    ),
                    y=alt.Y("publication_count:Q", title="Number of Publications"),
                    color=alt.Color("oa_color:N", title="OA Status"),
                    xOffset=alt.XOffset("oa_color:N"),
                )
            )

            text_labels = (
                alt.Chart(total_labels_df)
                .mark_text(
                    align="center",
                    baseline="bottom",
                    fontSize=14,
                    fontWeight="bold",
                    color="white",
                    lineBreak="\n",
                )
                .encode(
                    x="publication_year:O", y="max_bar_height:Q", text="label_text:N"
                )
            )

            vertical_connector = (
                alt.Chart(total_labels_df)
                .mark_rule(strokeWidth=1.5, color="white")
                .encode(
                    x="publication_year:O",
                    y=alt.Y("max_bar_height:Q"),
                    y2=alt.Y2("max_bar_height:Q"),
                )
            )

            horizontal_connector = (
                alt.Chart(total_labels_df)
                .mark_rule(strokeWidth=1.5, color="white")
                .encode(
                    x=alt.X("publication_year:O"),
                    x2=alt.X2("publication_year:O"),
                    y=alt.Y("max_bar_height:Q"),
                )
            )

            final_yearly_chart = (
                bar_chart + vertical_connector + horizontal_connector + text_labels
            ).properties(title=yearly_chart_title, width=alt.Step(80))

            # --- Step 3: Create the Totals Chart ---

            total_chart_title = f"{name}: Overall Totals"

            total_bars = (
                alt.Chart(total_counts)
                .mark_bar()
                .encode(
                    # Sort the bars by count for readability
                    x=alt.X("oa_color:N", title="OA Status", sort="-y"),
                    y=alt.Y("publication_count:Q", title="Total Publications"),
                )
            )

            total_bar_labels = total_bars.mark_text(
                align="center",
                baseline="bottom",
                dy=-4,  # Position text just above the bar
                color="white",
            ).encode(text="publication_count:Q")

            final_total_chart = (total_bars + total_bar_labels).properties(
                title=total_chart_title
            )

            # --- Step 4: Combine the two charts side-by-side ---

            # Convert each to a marimo object and place them in a horizontal stack
            combined_view = mo.hstack([
                mo.ui.altair_chart(final_yearly_chart),
                mo.ui.altair_chart(final_total_chart),
            ])
            chart_pairs.append(combined_view)
        return chart_pairs

    # mo.vstack(oa_charts())


@app.function
def drop_columns_that_are_all_null(_df: pl.DataFrame) -> pl.DataFrame:
    return _df[[s.name for s in _df if not (s.null_count() == _df.height)]]


@app.cell(column=1, hide_code=True)
def _():
    range_label = mo.md(f"Selected publication years: {filter_years.value}")
    mo.vstack([
        mo.md("""
    # Data retrieval & processing settings
    The settings below control how the data retrieval and processing pipeline is executed. Check the boxes to enable the various steps, and use the filters to limit what data is processed. At the bottom you'll find file pickers to select files with data to load from Pure and OILS.

    Once ready, press the start button at the top to run the script, and view the output below!
            """),
        start_button,
        run_openalex_queries,
        run_people_page_queries,
        merge_with_oils,
        use_titles,
        verbose_people_page_retrieval,
        filter_years,
        range_label,
        filter_faculty,
        pub_path,
        org_path,
        pers_path,
        oils_path,
    ])


@app.cell
def _():
    verbose = verbose_people_page_retrieval.value
    return (verbose,)


@app.cell(hide_code=True)
async def _(full_pipeline):
    if start_button.value:
        data = await full_pipeline(
            publications_path=pub_path.path(index=0) or publications_path,
            orgs_path=org_path.path(index=0) or orgs_path,
            persons_path=pers_path.path(index=0) or persons_path,
            oils_data_path=oils_path.path(index=0) or oils_data_path,
            filter_years=filter_years.value,
            use_titles=use_titles.value,
            run_people_page_queries=run_people_page_queries.value,
            run_openalex_queries=run_openalex_queries.value,
            filter_faculty=filter_faculty.value,
            merge_with_oils=merge_with_oils.value,
        )

    return (data,)


@app.cell
def _(data):
    data


@app.cell(column=2)
def _():
    OAI_PMH_VERBS = [
        "ListSets",
        "ListMetadataFormats",
        "ListRecords",
        "Identify",
    ]
    CERIF_ITEM_TYPES = [
        "cerif:Person",
        "cerif:OrgUnit",
        "cerif:Publication",
        "cerif:Product",
        "cerif:Patent",
        "cerif:Product",
        "cerif:Project",
        "cerif:Funding",
    ]
    CERIF_COLLECTIONS = [
        "openaire_cris_publications",
        "openaire_cris_persons",
        "openaire_cris_orgunits",
        "openaire_cris_funding",
        "openaire_cris_patents",
        "openaire_cris_projects",
        "openaire_cris_datasets",
        "openaire_cris_products",
        "datasets:all",
    ]


@app.cell
async def _():
    try:
        with Path("pure_data.pkl").open("rb") as f:
            pure_data = pickle.load(f)
    except Exception as e:
        print(
            f"Error loading pickle file: {e}\nRetrieving data from Pure API instead..."
        )
        pure_data = await retrieve_from_pure([
            "openaire_cris_publications",
            "openaire_cris_persons",
            "openaire_cris_orgunits",
        ])
        # store as pickle
        with Path("pure_data.pkl").open("wb") as f:
            pickle.dump(pure_data, f)

    return (pure_data,)


@app.function
async def retrieve_from_pure(selected_collections: list[str]) -> dict[str, list[dict]]:

    BASEURL = "https://ris.utwente.nl/ws/oai"
    SCHEMA = "oai_cerif_openaire"

    async def parse_response(records: dict, resume_url: str) -> tuple[list[dict], str]:
        results = records.get("record")
        if not isinstance(results, list):
            results = [results]

        if records.get("resumptionToken"):
            resumetoken = records.get("resumptionToken").get("#text")
            url = f"{resume_url}&resumptionToken={resumetoken}"
        else:
            url = None

        return results, url

    async def fetch_single(url: str, client, resume_url) -> tuple[list[dict], str]:
        try:
            r = await client.get(url)
        except Exception as e:
            print(f"Error fetching URL {url}: {e}")
            return [], None
        try:
            parsed = xmltodict.parse(r.text)
        except Exception as e:
            print(f"Error parsing XML from URL {url}: {e}")
            return [], None
        records = parsed["OAI-PMH"]["ListRecords"]
        return await parse_response(records, resume_url)

    async def get_collection_data(collection: str, client):
        url = f"{BASEURL}?verb=ListRecords&metadataPrefix={SCHEMA}&set={collection}"
        resume_url = url.split("&metadataPrefix", maxsplit=1)[0]

        col_result = {collection: []}
        while url:
            try:
                response, url = await fetch_single(url, client, resume_url)
            except Exception as e:
                print(f"Error processing collection {collection} at URL {url}: {e}")
                break
            col_result[collection].extend(response) if response else None
        return col_result

    results = defaultdict(list)
    async with httpx.AsyncClient(timeout=None) as client:
        results = await asyncio.gather(*[
            get_collection_data(collection, client)
            for collection in selected_collections
        ])

    return results


@app.cell
def _():
    # A helper to safely get a value from a potentially nested dictionary
    def safe_get(data: dict, keys: list, default=None):
        """Safely access a nested key in a dictionary."""
        for key in keys:
            if not isinstance(data, dict) or key not in data:
                return default
            data = data[key]
        return data

    def parse_enum(value: str | dict | None) -> str | None:
        """
        Parses a field that might be a controlled vocabulary URL or a dict with #text.
        Extracts the most meaningful part.
        """
        if value is None:
            return None
        # Handle dicts like {'#text': 'epub', '@scheme': '...'}
        if isinstance(value, dict):
            text_val = value.get("#text")
            return text_val.strip() if text_val else None
        # Handle string URLs
        if isinstance(value, str) and ("/" in value or "#" in value):
            # Take the last part of the path/fragment
            return value.split("/")[-1].split("#")[-1]
        return str(value)

    def get_text(value: str | dict | None) -> str | None:
        """Safely extracts the '#text' value from a dict or returns the string itself."""
        if value is None:
            return None
        if isinstance(value, dict):
            return value.get("#text")
        return str(value)

    def get_id(value: dict | None) -> str | None:
        """Safely extracts the '@id' value from a dict."""
        if isinstance(value, dict):
            return value.get("@id")
        return None

    def parse_person_name(name_dict: dict | None) -> tuple[str | None, str | None]:
        """Parses a PersonName dict into family and first names."""
        if not isinstance(name_dict, dict):
            return None, None
        family = get_text(name_dict.get("cerif:FamilyNames"))
        first = get_text(name_dict.get("cerif:FirstNames"))
        return family, first

    def parse_contributors(contrib_list: list | None) -> list[dict] | None:
        if not contrib_list:
            return None

        parsed_list = []
        for item in contrib_list:
            person_data = safe_get(item, ["cerif:Person"])
            if not person_data:
                continue

            family_names, first_names = parse_person_name(
                person_data.get("cerif:PersonName")
            )
            affiliation_data = safe_get(item, ["cerif:Affiliation", "cerif:OrgUnit"])

            parsed_list.append({
                "person_id": get_id(person_data),
                "family_names": family_names,
                "first_names": first_names,
                "affiliation_id": get_id(affiliation_data),
                "affiliation_name": get_text(
                    safe_get(affiliation_data, ["cerif:Name"])
                ),
            })

        return parsed_list if parsed_list else None

    def ensure_list(value) -> list:
        """Ensures the returned value is a list, wrapping single items."""
        if value is None:
            return []
        if isinstance(value, list):
            return value
        return [value]

    return (
        ensure_list,
        get_id,
        get_text,
        parse_contributors,
        parse_enum,
        parse_person_name,
        safe_get,
    )


@app.cell
def _(ensure_list, get_id, get_text, parse_contributors, parse_enum, safe_get):
    def parse_publication(pub: dict) -> dict:
        """Parses a single publication dictionary into a clean, flat format."""
        return {
            "id": get_id(pub),
            "type": parse_enum(pub.get("pubt:Type")),
            "language": get_text(pub.get("cerif:Language")),
            "title": get_text(pub.get("cerif:Title")),
            "publication_date": get_text(pub.get("cerif:PublicationDate")),
            "doi": get_text(pub.get("cerif:DOI")),
            "url": get_text(pub.get("cerif:URL")),
            "abstract": get_text(pub.get("cerif:Abstract")),
            "volume": get_text(pub.get("cerif:Volume")),
            "issue": get_text(pub.get("cerif:Issue")),
            "start_page": get_text(pub.get("cerif:StartPage")),
            "end_page": get_text(pub.get("cerif:EndPage")),
            "status": parse_enum(pub.get("cerif:Status")),
            "access_right": parse_enum(pub.get("ar:Access")),
            "license": parse_enum(pub.get("cerif:License")),
            # Nested Lists and Objects
            "authors": parse_contributors(
                ensure_list(safe_get(pub, ["cerif:Authors", "cerif:Author"]))
            ),
            "editors": parse_contributors(
                ensure_list(safe_get(pub, ["cerif:Editors", "cerif:Editor"]))
            ),
            "keywords": [
                get_text(kw)
                for kw in ensure_list(pub.get("cerif:Keyword"))
                if get_text(kw)
            ],
            "isbn": [
                get_text(i) for i in ensure_list(pub.get("cerif:ISBN")) if get_text(i)
            ],
            "issn": [
                get_text(i) for i in ensure_list(pub.get("cerif:ISSN")) if get_text(i)
            ],
            # Linked entities (extracting ID and a descriptive name)
            "publisher_name": get_text(
                safe_get(
                    pub,
                    [
                        "cerif:Publishers",
                        "cerif:Publisher",
                        "cerif:OrgUnit",
                        "cerif:Name",
                    ],
                )
            ),
            "published_in_id": get_id(
                safe_get(pub, ["cerif:PublishedIn", "cerif:Publication"])
            ),
            "published_in_title": get_text(
                safe_get(pub, ["cerif:PublishedIn", "cerif:Publication", "cerif:Title"])
            ),
            "part_of_id": get_id(safe_get(pub, ["cerif:PartOf", "cerif:Publication"])),
            "part_of_title": get_text(
                safe_get(pub, ["cerif:PartOf", "cerif:Publication", "cerif:Title"])
            ),
            # Event Information
            "event_name": get_text(
                safe_get(pub, ["cerif:PresentedAt", "cerif:Event", "cerif:Name"])
            ),
            "event_acronym": get_text(
                safe_get(pub, ["cerif:PresentedAt", "cerif:Event", "cerif:Acronym"])
            ),
            "event_start_date": get_text(
                safe_get(pub, ["cerif:PresentedAt", "cerif:Event", "cerif:StartDate"])
            ),
            "event_end_date": get_text(
                safe_get(pub, ["cerif:PresentedAt", "cerif:Event", "cerif:EndDate"])
            ),
        }

    return (parse_publication,)


@app.cell
def _(get_id, get_text, parse_person_name, safe_get):
    def parse_person(pers: dict) -> dict:
        """Parses a single person dictionary into a clean, flat format."""
        family_names, first_names = parse_person_name(pers.get("cerif:PersonName"))
        return {
            "id": get_id(pers),
            "family_names": family_names,
            "first_names": first_names,
            "orcid": get_text(pers.get("cerif:ORCID")),
            "scopus_author_id": get_text(pers.get("cerif:ScopusAuthorID")),
            "researcher_id": get_text(pers.get("cerif:ResearcherID")),
            "affiliation_id": get_id(
                safe_get(pers, ["cerif:Affiliation", "cerif:OrgUnit"])
            ),
            "affiliation_name": get_text(
                safe_get(pers, ["cerif:Affiliation", "cerif:OrgUnit", "cerif:Name"])
            ),
        }

    return (parse_person,)


@app.cell
def _(get_id, get_text, parse_enum, safe_get):
    def parse_orgunit(org: dict) -> dict:
        """Parses a single organization dictionary into a clean, flat format."""
        return {
            "id": get_id(org),
            "name": get_text(org.get("cerif:Name")),
            "acronym": get_text(org.get("cerif:Acronym")),
            "type": parse_enum(org.get("cerif:Type")),
            "identifier": get_text(org.get("cerif:Identifier")),
            "identifier_type": parse_enum(safe_get(org, ["cerif:Identifier", "@type"])),
            "part_of_org_id": get_id(safe_get(org, ["cerif:PartOf", "cerif:OrgUnit"])),
            "email": get_text(org.get("cerif:ElectronicAddress")),
        }

    return (parse_orgunit,)


@app.cell
def _(pure_data):
    # Now check which fields are retrieved for each item type to create the "schema"
    # then create functions to parse this data into proper dicts for each item type
    # in order to create dataframes from this data.
    publications = [
        pub["metadata"]["cerif:Publication"]
        for pub in pure_data[0]["openaire_cris_publications"]
        if pub.get("metadata", {}).get("cerif:Publication")
    ]
    persons = [
        pub["metadata"]["cerif:Person"]
        for pub in pure_data[1]["openaire_cris_persons"]
        if pub.get("metadata", {}).get("cerif:Person")
    ]
    organizations = [
        pub["metadata"]["cerif:OrgUnit"]
        for pub in pure_data[2]["openaire_cris_orgunits"]
        if pub.get("metadata", {}).get("cerif:OrgUnit")
    ]

    def parse_dict(d: dict):
        result = {}
        for k, v in d.items():
            if isinstance(v, dict):
                result[k] = parse_dict(v)
            elif isinstance(v, list):
                result[k] = [
                    parse_dict(item) if isinstance(item, dict) else type(item)
                    for item in v
                ]
            else:
                result[k] = type(v)
        return result

    def parse_dict_val(d: dict):
        result = {}
        for k, v in d.items():
            if isinstance(v, dict):
                result[k] = parse_dict_val(v)
            elif isinstance(v, list):
                result[k] = [
                    parse_dict_val(item) if isinstance(item, dict) else item
                    for item in v
                ]
            else:
                result[k] = v
        return result

    pubs = []
    pers = []
    orgs = []

    for p in publications:
        if isinstance(p, list):
            pubs.extend(p)
        else:
            pubs.append(p)
    for p in persons:
        if isinstance(p, list):
            pers.extend(p)
        else:
            pers.append(p)
    for o in organizations:
        if isinstance(o, list):
            orgs.extend(o)
        else:
            orgs.append(o)

    lens = defaultdict(int)
    keyset = {}
    valueset = {}
    for name, collection in zip(["pubs", "pers", "orgs"], [pubs, pers, orgs]):
        keys = {}
        values = {}
        for p in collection:
            if not isinstance(p, dict):
                lens[str(type(p))] += 1
            else:
                lens[len(p)] += 1
                keys.update(parse_dict(p))
                values.update(parse_dict_val(p))
        keyset[name] = keys
        valueset[name] = values

    print(valueset)
    return orgs, parse_dict, parse_dict_val, pers, pubs


@app.cell
def _(
    orgs,
    parse_dict,
    parse_dict_val,
    parse_orgunit,
    parse_person,
    parse_publication,
    pers,
    pubs,
):
    parsed_publications = [parse_publication(p) for p in pubs]
    parsed_persons = [parse_person(p) for p in pers]
    parsed_orgs = [parse_orgunit(o) for o in orgs]

    def check_not_none(d: dict):
        final = {}
        if isinstance(d, list):
            res = [check_not_none(item) for item in d]
            if not all([x == type(None) for x in res]):
                return res
            return type(None)
        if isinstance(d, dict):
            for k, v in d.items():
                if (not v == type(None)) and v:
                    if isinstance(v, list):
                        res = check_not_none(v)
                        if not isinstance(res, list):
                            continue
                        if all([x == type(None) for x in res]):
                            continue
                        final[k] = res
                    elif isinstance(v, dict):
                        res = check_not_none(v)
                        if all([x == type(None) for x in res.values()]):
                            continue
                        final[k] = res
                    elif v != type(None):
                        final[k] = v
        elif d != type(None):
            return d
        else:
            return None
        return final

    def check_data(collections):
        lens = defaultdict(int)
        keyset = {}
        valueset = {}
        for name, collection in collections:
            keys = {}
            values = {}
            for p in collection:
                if not isinstance(p, dict):
                    lens[str(type(p))] += 1
                else:
                    lens[len(p)] += 1
                    data = parse_dict(p)
                    keys.update(check_not_none(data))
                    val_data = parse_dict_val(p)
                    values.update(check_not_none(val_data))
            keyset[name] = keys
            valueset[name] = values

        print(keyset)
        print(valueset)

    check_data(
        zip(
            ["pubs", "pers", "orgs"], [parsed_publications, parsed_persons, parsed_orgs]
        )
    )

    df_publications = pl.DataFrame(parsed_publications, infer_schema_length=0)
    df_persons = pl.DataFrame(parsed_persons, infer_schema_length=0)
    df_orgs = pl.DataFrame(parsed_orgs, infer_schema_length=0)


if __name__ == "__main__":
    app.run()
