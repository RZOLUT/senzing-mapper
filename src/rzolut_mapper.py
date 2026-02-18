#! /usr/bin/env python3
import argparse
import ast
import csv
import hashlib
import json
import os
import random
import signal
import sys
import time
from datetime import datetime
from itertools import zip_longest


# =========================
class mapper:

    # ----------------------------------------
    def __init__(self):

        self.load_reference_data()
        self.stat_pack = {}

    # ----------------------------------------
    def map(self, raw_data, input_row_num=None):

        # Clean the raw data values using the clean_value method
        for attribute in raw_data:
            raw_data[attribute] = self.clean_value(raw_data[attribute])

        # Set essential fields for the JSON data
        json_data = {"DATA_SOURCE": args.data_source, "RECORD_ID": raw_data["uid"], "FEATURES": []}

        # Record type: PERSON or ORGANIZATION (now in FEATURES)
        record_type = "PERSON" if raw_data.get("subject_type", "") == "Individual" else "ORGANIZATION"
        json_data["FEATURES"].append({"RECORD_TYPE": record_type})

        # Store the subject_type as root payload
        json_data["subject_type"] = raw_data.get("subject_type")

        # Set primary names (now in FEATURES with correct attribute names)
        if record_type == "PERSON":
            name_feat = {"NAME_TYPE": "PRIMARY"}
            if raw_data.get("first_name"):
                name_feat["NAME_FIRST"] = raw_data["first_name"]
            if raw_data.get("middle_name"):
                name_feat["NAME_MIDDLE"] = raw_data["middle_name"]
            if raw_data.get("last_name"):
                name_feat["NAME_LAST"] = raw_data["last_name"]
            if len(name_feat) > 1:
                json_data["FEATURES"].append(name_feat)
        else:
            if raw_data.get("name"):
                json_data["FEATURES"].append({"NAME_TYPE": "PRIMARY", "NAME_ORG": raw_data["name"]})

        # Append gender information if available
        if raw_data.get("gender", ""):
            json_data["FEATURES"].append({"GENDER": raw_data.get("gender", "")})

        # Process image URLs -> root payload (not FEATURES)
        if raw_data.get("image_url"):
            img = raw_data.get("image_url", "")
            if img.strip():
                json_data["image_url"] = img.strip()

        # Process date of birth information
        date_of_birth_year_list = raw_data.get("date_of_birth_year", []) or []
        date_of_birth_month_list = raw_data.get("date_of_birth_month", []) or []
        date_of_birth_date_list = raw_data.get("date_of_birth_date", []) or []

        for year, month, date in zip_longest(
            date_of_birth_year_list,
            date_of_birth_month_list,
            date_of_birth_date_list,
            fillvalue="",
        ):
            try:
                y = self.clean_val(year)
                m = self.clean_val(month)
                d = self.clean_val(date)

                if y and m and d:
                    json_data["FEATURES"].append({"DATE_OF_BIRTH": f"{y}-{m}-{d}"})
                elif y and m:
                    json_data["FEATURES"].append({"DATE_OF_BIRTH": f"{y}-{m}"})
                elif m and d:
                    json_data["FEATURES"].append({"DATE_OF_BIRTH": f"{m}/{d}"})
                elif y:
                    json_data["FEATURES"].append({"DATE_OF_BIRTH": y})
            except Exception as ex:
                print(f"id {raw_data['uid']} date_of_birth parse error {ex}")

        # Process date of death information
        date_of_death_year_list = raw_data.get("date_of_death_year", []) or []
        date_of_death_month_list = raw_data.get("date_of_death_month", []) or []
        date_of_death_date_list = raw_data.get("date_of_death_date", []) or []
        is_deceased = raw_data.get("deceased_status", "")

        # Fix: deceased_status -> root payload (was incorrectly mapped to "country")
        if is_deceased:
            json_data["deceased_status"] = is_deceased

        if is_deceased:
            for year, month, date in zip_longest(
                date_of_death_year_list,
                date_of_death_month_list,
                date_of_death_date_list,
                fillvalue="",
            ):
                try:
                    y = self.clean_val(year)
                    m = self.clean_val(month)
                    d = self.clean_val(date)

                    if y and m and d:
                        json_data["FEATURES"].append({"DATE_OF_DEATH": f"{y}-{m}-{d}"})
                    elif y and m:
                        json_data["FEATURES"].append({"DATE_OF_DEATH": f"{y}-{m}"})
                    elif m and d:
                        json_data["FEATURES"].append({"DATE_OF_DEATH": f"{m}/{d}"})
                    elif y:
                        json_data["FEATURES"].append({"DATE_OF_DEATH": y})
                except Exception as ex:
                    print(f"id {raw_data['uid']} date_of_death parse error {ex}")

        # Retrieve and split address-related data into lists
        address_type_list = raw_data.get("address_type", []) or []
        address_street_list = raw_data.get("address_street", []) or []
        address_city_list = raw_data.get("address_city", []) or []
        address_province_list = raw_data.get("address_province", []) or []
        address_postal_code_list = raw_data.get("address_postal_code", []) or []
        address_country_list = raw_data.get("address_country", []) or []
        address_country_code_list = raw_data.get("address_country_code", []) or []

        for (
            addr_type,
            street,
            country,
            city,
            province,
            postal,
            country_code,
        ) in zip_longest(
            address_type_list,
            address_street_list,
            address_country_list,
            address_city_list,
            address_province_list,
            address_postal_code_list,
            address_country_code_list,
            fillvalue="",
        ):
            try:
                # Fix: removed ADDR_LINE2 = country (wrong mapping)
                _data = {
                    "ADDR_TYPE": self.clean_val(addr_type),
                    "ADDR_LINE1": self.clean_val(street),
                    "ADDR_CITY": self.clean_val(city),
                    "ADDR_STATE": self.clean_val(province),
                    "ADDR_POSTAL_CODE": self.clean_val(postal),
                    "ADDR_COUNTRY": self.clean_val(country_code),
                }

                if any(v for v in _data.values()):
                    json_data["FEATURES"].append(_data)

            except Exception as ex:
                print(f"id {raw_data.get('uid')} address parse error {ex}")

        # Set SOE status based on raw data
        json_data["soe_status"] = "Yes" if "Yes" in raw_data.get("soe_status", "") else ""

        # Retrieve and split PEP-related data into lists
        pep_type_list = raw_data.get("pep_type", []) or []
        pep_level_list = raw_data.get("pep_level", []) or []
        position_list = raw_data.get("position", []) or []
        org_name_list = raw_data.get("organization_name", []) or []

        position_start_date_year_list = raw_data.get("position_start_date_year", []) or []
        position_start_date_month_list = raw_data.get("position_start_date_month", []) or []
        position_start_date_date_list = raw_data.get("position_start_date_date", []) or []

        position_end_date_year_list = raw_data.get("position_end_date_year", []) or []
        position_end_date_month_list = raw_data.get("position_end_date_month", []) or []
        position_end_date_date_list = raw_data.get("position_end_date_date", []) or []

        # Add group associations with GROUP_ASSOCIATION_TYPE
        for name in org_name_list:
            if name.strip() and name != "~":
                json_data["FEATURES"].append(
                    {"GROUP_ASSOCIATION_TYPE": "MEMBER", "GROUP_ASSOCIATION_ORG_NAME": name.strip()}
                )

        # PEP positions -> payload stringified list
        positions = []
        for (
            pep_type,
            pep_level,
            position,
            org_name,
            start_y,
            start_m,
            start_d,
            end_y,
            end_m,
            end_d,
        ) in zip_longest(
            pep_type_list,
            pep_level_list,
            position_list,
            org_name_list,
            position_start_date_year_list,
            position_start_date_month_list,
            position_start_date_date_list,
            position_end_date_year_list,
            position_end_date_month_list,
            position_end_date_date_list,
            fillvalue="",
        ):
            try:
                _data = {
                    "pep_types": self.clean_val(pep_type),
                    "pep_level": self.clean_val(pep_level),
                    "position": self.clean_val(position),
                    "position_organization": self.clean_val(org_name),
                    "position_start_year": self.clean_val(start_y),
                    "position_start_month": self.clean_val(start_m),
                    "position_start_date": self.clean_val(start_d),
                    "position_end_year": self.clean_val(end_y),
                    "position_end_month": self.clean_val(end_m),
                    "position_end_date": self.clean_val(end_d),
                }

                if any(value for value in _data.values()):
                    positions.append(_data)

            except Exception as ex:
                print(f"id {raw_data.get('uid', 'unknown')} pep details parse error {ex}")

        if positions:
            json_data["positions"] = self.clean_json_dumps(positions)

        # Process alias names with corrected attribute names
        if raw_data.get("alias_name"):
            alias_names = raw_data.get("alias_name", []) or []
            alias_types = raw_data.get("alias_type", []) or []
            alias_scripts = raw_data.get("alias_script", []) or []
            alias_languages = raw_data.get("alias_language", []) or []

            for alias_name, alias_type, alias_script, alias_language in zip_longest(
                alias_names, alias_types, alias_scripts, alias_languages
            ):
                alias_name = self.clean_val(alias_name)

                if alias_name:
                    if record_type == "PERSON":
                        json_data["FEATURES"].append({"NAME_TYPE": "AKA", "NAME_FULL": alias_name})
                    else:
                        json_data["FEATURES"].append({"NAME_TYPE": "AKA", "NAME_ORG": alias_name})

        # Retrieve relationship-related data
        relationship_subject_type_list = raw_data.get("association_subject_type", []) or []
        relationship_name_list = raw_data.get("association_name", []) or []
        relationship_type_list = raw_data.get("association_relationship_type", []) or []
        relationship_type_desc_list = raw_data.get("association_relationship_type_description", []) or []
        relationship_uid_list = raw_data.get("association_relationship_uid", []) or []

        # Add one REL_ANCHOR per record (always, since other records may point to this one)
        json_data["FEATURES"].append({"REL_ANCHOR_DOMAIN": args.data_source, "REL_ANCHOR_KEY": raw_data["uid"]})

        # Build relationship details payload and proper REL_POINTER features
        relationship_details = []
        for rel_subject_type, rel_name, rel_type, rel_type_desc, rel_uid in zip_longest(
            relationship_subject_type_list,
            relationship_name_list,
            relationship_type_list,
            relationship_type_desc_list,
            relationship_uid_list,
        ):
            try:
                rel_subject_type = self.clean_val(rel_subject_type)
                rel_name = self.clean_val(rel_name)
                rel_type = self.clean_val(rel_type)
                rel_type_desc = self.clean_val(rel_type_desc)
                rel_uid = self.clean_val(rel_uid)

                _detail = {
                    "subject_type": rel_subject_type,
                    "name": rel_name,
                    "type": rel_type,
                    "type_description": rel_type_desc,
                    "uid": rel_uid,
                }
                if any(_detail.values()):
                    relationship_details.append(_detail)

                # Proper REL_POINTER
                if rel_uid:
                    pointer = {
                        "REL_POINTER_DOMAIN": args.data_source,
                        "REL_POINTER_KEY": rel_uid,
                    }
                    if rel_type:
                        pointer["REL_POINTER_ROLE"] = rel_type
                    json_data["FEATURES"].append(pointer)

            except Exception as ex:
                print(f"id {raw_data.get('uid')} relationship parse error {ex}")

        if relationship_details:
            json_data["relationship_details"] = self.clean_json_dumps(relationship_details)

        # PEP countries -> payload stringified list
        pep_country_list = raw_data.get("pep_country", []) or []
        pep_country_code_list = raw_data.get("pep_country_code", []) or []

        pep_countries = []
        for pep_country, pep_country_code in zip_longest(pep_country_list, pep_country_code_list):
            pep_country = self.clean_val(pep_country)
            pep_country_code = self.clean_val(pep_country_code)

            _data = {"country": pep_country, "country_code": pep_country_code}
            if any(_data.values()):
                pep_countries.append(_data)

        if pep_countries:
            json_data["pep_countries"] = self.clean_json_dumps(pep_countries)

        # Sources -> payload stringified list
        source_type_list = raw_data.get("source_type", []) or []
        source_list = raw_data.get("external_sources", []) or []
        source_description_list = raw_data.get("source_description", []) or []

        sources = []
        for source_type, source, source_description in zip_longest(
            source_type_list, source_list, source_description_list
        ):
            source_type = self.clean_val(source_type)
            source = self.clean_val(source)
            source_description = self.clean_val(source_description)

            _data = {"source_type": source_type, "source": source, "source_description": source_description}
            if any(_data.values()):
                sources.append(_data)

        if sources:
            json_data["sources"] = self.clean_json_dumps(sources)

        # Add timestamps to json_data
        json_data["CREATED_AT"] = raw_data["entered"]
        json_data["UPDATED_AT"] = raw_data["updated"]

        # Process citizenship -> fix to just CITIZENSHIP with country_code preferred
        citizenship_country_list = raw_data.get("citizenship", []) or []
        citizenship_country_code_list = raw_data.get("citizenship_country_code", []) or []

        for citizenship_country, citizenship_country_code in zip_longest(
            citizenship_country_list, citizenship_country_code_list
        ):
            citizenship_country = self.clean_val(citizenship_country)
            citizenship_country_code = self.clean_val(citizenship_country_code)

            val = citizenship_country_code or citizenship_country
            if val:
                json_data["FEATURES"].append({"CITIZENSHIP": val})

        # Process nationality -> fix to just NATIONALITY with country_code preferred
        nationality_country_list = raw_data.get("nationality_country", []) or []
        nationality_country_code_list = raw_data.get("nationality_country_code", []) or []

        for nationality_country, nationality_country_code in zip_longest(
            nationality_country_list, nationality_country_code_list
        ):
            nationality_country = self.clean_val(nationality_country)
            nationality_country_code = self.clean_val(nationality_country_code)

            val = nationality_country_code or nationality_country
            if val:
                json_data["FEATURES"].append({"NATIONALITY": val})

        # Process identifiers
        identifier_name_list = raw_data.get("identifier_name", []) or []
        identifier_value_list = raw_data.get("identifier_value", []) or []
        identifier_country_list = raw_data.get("identifier_country", []) or []
        identifier_country_code_list = raw_data.get("identifier_country_code", []) or []
        identifier_issuing_authority_list = raw_data.get("identifier_issuing_authority", []) or []

        identifier_issue_date_date_list = raw_data.get("identifier_issue_date_date", []) or []
        identifier_issue_date_month_list = raw_data.get("identifier_issue_date_month", []) or []
        identifier_issue_date_year_list = raw_data.get("identifier_issue_date_year", []) or []

        identifier_expiry_date_date_list = raw_data.get("identifier_expiry_date_date", []) or []
        identifier_expiry_date_month_list = raw_data.get("identifier_expiry_date_month", []) or []
        identifier_expiry_date_year_list = raw_data.get("identifier_expiry_date_year", []) or []

        for (
            raw_type,
            value,
            country,
            country_code,
            issuing_authority,
            issue_y,
            issue_m,
            issue_d,
            expiry_y,
            expiry_m,
            expiry_d,
        ) in zip_longest(
            identifier_name_list,
            identifier_value_list,
            identifier_country_list,
            identifier_country_code_list,
            identifier_issuing_authority_list,
            identifier_issue_date_year_list,
            identifier_issue_date_month_list,
            identifier_issue_date_date_list,
            identifier_expiry_date_year_list,
            identifier_expiry_date_month_list,
            identifier_expiry_date_date_list,
            fillvalue="",
        ):
            try:
                raw_type = self.clean_val(raw_type).upper()
                value = self.clean_val(value).upper().lstrip("0")
                country = self.clean_val(country)
                country_code = self.clean_val(country_code)
                issuing_authority = self.clean_val(issuing_authority)
                identifier_issue_date = ""
                identifier_expiry_date = ""

                try:
                    y = self.clean_val(issue_y)
                    m = self.clean_val(issue_m)
                    d = self.clean_val(issue_d)

                    if y and m and d:
                        identifier_issue_date = f"{y}-{m}-{d}"
                    elif y and m:
                        identifier_issue_date = f"{y}-{m}"
                    elif m and d:
                        identifier_issue_date = f"{m}/{d}"
                    elif y:
                        identifier_issue_date = y
                except Exception as ex:
                    print(f"id {raw_data['uid']} identifier_issue_date parse error {ex}")

                try:
                    y = self.clean_val(expiry_y)
                    m = self.clean_val(expiry_m)
                    d = self.clean_val(expiry_d)

                    if y and m and d:
                        identifier_expiry_date = f"{y}-{m}-{d}"
                    elif y and m:
                        identifier_expiry_date = f"{y}-{m}"
                    elif m and d:
                        identifier_expiry_date = f"{m}/{d}"
                    elif y:
                        identifier_expiry_date = y
                except Exception as ex:
                    print(f"id {raw_data['uid']} identifier_expiry_date parse error {ex}")

                # Update statistics for identifier type
                self.update_stat("!IDTYPE", raw_type, value)

                if raw_type == "LEGAL ENTITY IDENTIFIER (LEI)":
                    json_data["FEATURES"].append({"LEI_NUMBER": value})

                elif raw_type == "DRIVER'S LICENSE NUMBER":
                    json_data["FEATURES"].append(
                        {
                            "DRIVERS_LICENSE_NUMBER": value,
                            "DRIVERS_LICENSE_STATE": country_code,
                        }
                    )

                elif raw_type == "SOCIAL SECURITY NUMBER (SSN)":
                    json_data["FEATURES"].append({"SSN_NUMBER": value})

                elif raw_type == "NATIONAL PROVIDER IDENTIFIER":
                    json_data["FEATURES"].append({"NPI_NUMBER": value})

                # Fix: drop date attributes from passport
                elif raw_type == "PASSPORT NUMBER":
                    json_data["FEATURES"].append(
                        {
                            "PASSPORT_NUMBER": value,
                            "PASSPORT_COUNTRY": country_code,
                        }
                    )
                elif raw_type == "DIRECTOR IDENTIFICATION NUMBER (DIN)":
                    json_data["FEATURES"].append(
                        {"NATIONAL_ID_TYPE": "DIN", "NATIONAL_ID_NUMBER": value, "NATIONAL_ID_COUNTRY": country_code}
                    )
                elif raw_type == "CORPORATE IDENTIFICATION NUMBER (CIN)":
                    json_data["FEATURES"].append(
                        {"NATIONAL_ID_TYPE": "CIN", "NATIONAL_ID_NUMBER": value, "NATIONAL_ID_COUNTRY": country_code}
                    )
                elif raw_type == "LIMITED LIABILITY PARTNERSHIP IDENTIFICATION NUMBER (LLPIN)":
                    json_data["FEATURES"].append(
                        {"NATIONAL_ID_TYPE": "LLPIN", "NATIONAL_ID_NUMBER": value, "NATIONAL_ID_COUNTRY": country_code}
                    )
                elif raw_type == "FCRN NUMBER":
                    json_data["FEATURES"].append(
                        {"NATIONAL_ID_TYPE": "FCRN", "NATIONAL_ID_NUMBER": value, "NATIONAL_ID_COUNTRY": country_code}
                    )
                elif raw_type == "FIRM REGISTRATION NUMBER (FRN)":
                    json_data["FEATURES"].append(
                        {"NATIONAL_ID_TYPE": "FRN", "NATIONAL_ID_NUMBER": value, "NATIONAL_ID_COUNTRY": country_code}
                    )
                elif raw_type == "CEDULA NUMBER":
                    json_data["FEATURES"].append(
                        {"NATIONAL_ID_TYPE": "CEDULA", "NATIONAL_ID_NUMBER": value, "NATIONAL_ID_COUNTRY": country_code}
                    )
                elif raw_type == "PRIMARY STATE REGISTRATION NUMBER (OGRN)":
                    json_data["FEATURES"].append(
                        {"NATIONAL_ID_TYPE": "OGRN", "NATIONAL_ID_NUMBER": value, "NATIONAL_ID_COUNTRY": country_code}
                    )
                elif raw_type == "SYST\u00c8ME D'IDENTIFICATION DU R\u00c9PERTOIRE DES ENTREPRISES (SIREN) NUMBER":
                    json_data["FEATURES"].append(
                        {"NATIONAL_ID_TYPE": "SIREN", "NATIONAL_ID_NUMBER": value, "NATIONAL_ID_COUNTRY": country_code}
                    )
                # Fix: drop date attributes from PAN tax_id
                elif raw_type == "PERMANENT ACCOUNT NUMBER (PAN)":
                    json_data["FEATURES"].append(
                        {
                            "TAX_ID_TYPE": "PAN",
                            "TAX_ID_NUMBER": value,
                            "TAX_ID_COUNTRY": country_code,
                        }
                    )
                # Fix: drop date attributes from LICENSE
                elif raw_type == "LICENSE NUMBER":
                    json_data["FEATURES"].append(
                        {
                            "OTHER_ID_TYPE": "LICENSE",
                            "OTHER_ID_NUMBER": value,
                            "OTHER_ID_COUNTRY": country_code,
                        }
                    )
                elif raw_type == "CADASTRO NACIONAL DA PESSOA JUR\u00cdDICA (CNPJ)":
                    json_data["FEATURES"].append(
                        {"TAX_ID_TYPE": "CNPJ", "TAX_ID_NUMBER": value, "TAX_ID_COUNTRY": country_code}
                    )
                elif raw_type == "GST NUMBER":
                    json_data["FEATURES"].append(
                        {"TAX_ID_TYPE": "GST", "TAX_ID_NUMBER": value, "TAX_ID_COUNTRY": country_code}
                    )
                elif raw_type == "TAX IDENTIFICATION NUMBER (TIN)":
                    json_data["FEATURES"].append(
                        {"TAX_ID_TYPE": "TIN", "TAX_ID_NUMBER": value, "TAX_ID_COUNTRY": country_code}
                    )
                elif raw_type == "CADASTRO DE PESSOAS F\u00cdSICAS (CPF)":
                    json_data["FEATURES"].append(
                        {"TAX_ID_TYPE": "CPF", "TAX_ID_NUMBER": value, "TAX_ID_COUNTRY": country_code}
                    )
                elif raw_type == "INN NUMBER":
                    json_data["FEATURES"].append(
                        {"TAX_ID_TYPE": "INN", "TAX_ID_NUMBER": value, "TAX_ID_COUNTRY": country_code}
                    )
                elif raw_type == "VALUE ADDED TAX NUMBER (VAT)":
                    json_data["FEATURES"].append(
                        {"TAX_ID_TYPE": "VAT", "TAX_ID_NUMBER": value, "TAX_ID_COUNTRY": country_code}
                    )
                # Fix: drop date attributes from fallback OTHER_ID
                else:
                    json_data["FEATURES"].append(
                        {
                            "OTHER_ID_TYPE": raw_type,
                            "OTHER_ID_NUMBER": value,
                            "OTHER_ID_COUNTRY": country_code,
                        }
                    )
            except Exception as ex:
                print(f"id {raw_data['uid']} identifier parse error {ex}")

        # Vessels -> payload stringified list
        vessel_type_list = raw_data.get("vessel_type", []) or []
        current_country_flag_list = raw_data.get("current_country_flag", []) or []
        former_country_flag_list = raw_data.get("former_country_flag", []) or []

        vessels = []
        for vessel_type, curr_country, form_country in zip_longest(
            vessel_type_list, current_country_flag_list, former_country_flag_list
        ):
            vessel_type = self.clean_val(vessel_type)
            curr_country = self.clean_val(curr_country)
            form_country = self.clean_val(form_country)

            _data = {
                "vessel_type": vessel_type,
                "current_country_flag": curr_country,
                "former_country_flag": form_country,
            }
            if any(_data.values()):
                vessels.append(_data)

        if vessels:
            json_data["vessels"] = self.clean_json_dumps(vessels)

        # Aircraft -> payload stringified list
        aircraft_manufacture_date_date_list = raw_data.get("aircraft_manufacture_date_date", []) or []
        aircraft_manufacture_date_month_list = raw_data.get("aircraft_manufacture_date_month", []) or []
        aircraft_manufacture_date_year_list = raw_data.get("aircraft_manufacture_date_year", []) or []
        aircraft_model = raw_data.get("aircraft_model", "")

        aircraft = []
        for d, m, y, model in zip_longest(
            aircraft_manufacture_date_date_list,
            aircraft_manufacture_date_month_list,
            aircraft_manufacture_date_year_list,
            aircraft_model,
            fillvalue="",
        ):
            _data = {
                "manufacture_date": self.clean_val(d),
                "manufacture_month": self.clean_val(m),
                "manufacture_year": self.clean_val(y),
                "model": self.clean_val(model),
            }
            if any(_data.values()):
                aircraft.append(_data)

        if aircraft:
            json_data["aircraft"] = self.clean_json_dumps(aircraft)

        # Extract incorporation date information -> REGISTRATION_DATE (keep in FEATURES)
        date_of_incorporation_year_list = raw_data.get("date_of_incorporation_year", []) or []
        date_of_incorporation_month_list = raw_data.get("date_of_incorporation_month", []) or []
        date_of_incorporation_date_list = raw_data.get("date_of_incorporation_date", []) or []

        for year, month, date in zip_longest(
            date_of_incorporation_year_list,
            date_of_incorporation_month_list,
            date_of_incorporation_date_list,
            fillvalue="",
        ):
            try:
                y = self.clean_val(year)
                m = self.clean_val(month)
                d = self.clean_val(date)
                if y and m and d:
                    json_data["FEATURES"].append({"REGISTRATION_DATE": f"{y}-{m}-{d}"})
                elif y and m:
                    json_data["FEATURES"].append({"REGISTRATION_DATE": f"{y}-{m}"})
                elif m and d:
                    json_data["FEATURES"].append({"REGISTRATION_DATE": f"{m}/{d}"})
                elif y:
                    json_data["FEATURES"].append({"REGISTRATION_DATE": y})
            except Exception as ex:
                print(f"id {raw_data['uid']} date_of_incorporation parse error {ex}")

        # REGISTRATION_COUNTRY -> keep in FEATURES (correct)
        country_code_of_incorporation_list = raw_data.get("country_code_of_incorporation", []) or []
        for incorporation_code in country_code_of_incorporation_list:
            incorporation_code = self.clean_val(incorporation_code)
            if incorporation_code:
                json_data["FEATURES"].append({"REGISTRATION_COUNTRY": incorporation_code})

        # Country of origin -> payload stringified list
        country_code_of_origin_list = raw_data.get("country_code_of_origin", []) or []
        countries_of_origin = []
        for origin_code in country_code_of_origin_list:
            origin_code = self.clean_val(origin_code)
            if origin_code:
                countries_of_origin.append(origin_code)
        if countries_of_origin:
            json_data["countries_of_origin"] = self.clean_json_dumps(countries_of_origin)

        # Ownership details (shareholding) -> payload stringified list
        percentage_of_shareholding_list = raw_data.get("association_percentage_of_shareholding", []) or []
        shareholdings = []
        for shareholding in percentage_of_shareholding_list:
            shareholding = self.clean_val(shareholding)
            if shareholding:
                shareholdings.append(shareholding)
        if shareholdings:
            json_data["shareholdings"] = self.clean_json_dumps(shareholdings)

        # Age -> payload
        age_in_yrs_list = raw_data.get("age", []) or []
        ages = []
        for age in age_in_yrs_list:
            age = self.clean_val(age)
            if age:
                ages.append(age)
        if ages:
            json_data["ages"] = self.clean_json_dumps(ages)

        # PHONE_NUMBER -> keep in FEATURES (correct)
        contact_number_list = raw_data.get("contact_number", []) or []
        for phone in contact_number_list:
            phone = self.clean_val(phone)
            if phone:
                json_data["FEATURES"].append({"PHONE_NUMBER": phone})

        # EMAIL_ADDRESS -> keep in FEATURES (correct)
        email_id_list = raw_data.get("email_id", []) or []
        for email in email_id_list:
            email = self.clean_val(email)
            if email:
                json_data["FEATURES"].append({"EMAIL_ADDRESS": email})

        # WEBSITE_ADDRESS -> keep in FEATURES (correct)
        website_list = raw_data.get("website", []) or []
        for site in website_list:
            site = self.clean_val(site)
            if site:
                json_data["FEATURES"].append({"WEBSITE_ADDRESS": site})

        # Physical descriptions -> payload
        color_of_hair_list = raw_data.get("color_of_hair", []) or []
        color_of_eyes_list = raw_data.get("color_of_eyes", []) or []
        height_list = raw_data.get("height", []) or []
        weight_list = raw_data.get("weight", []) or []
        distinguishing_marks_list = raw_data.get("distinguishing_marks_and_characteristics", []) or []

        physical_descriptions = []
        for hair_color in color_of_hair_list:
            hair_color = self.clean_val(hair_color)
            if hair_color:
                physical_descriptions.append({"type": "hair_color", "value": hair_color})
        for eye_color in color_of_eyes_list:
            eye_color = self.clean_val(eye_color)
            if eye_color:
                physical_descriptions.append({"type": "eye_color", "value": eye_color})
        for height in height_list:
            height = self.clean_val(height)
            if height:
                physical_descriptions.append({"type": "height", "value": height})
        for weight in weight_list:
            weight = self.clean_val(weight)
            if weight:
                physical_descriptions.append({"type": "weight", "value": weight})
        for mark in distinguishing_marks_list:
            mark = self.clean_val(mark)
            if mark:
                physical_descriptions.append({"type": "distinguishing_marks", "value": mark})
        if physical_descriptions:
            json_data["physical_descriptions"] = self.clean_json_dumps(physical_descriptions)

        # Profile summaries -> payload stringified list
        profile_summary_list = raw_data.get("profile_summary", []) or []
        profile_summaries = []
        for summary in profile_summary_list:
            summary = self.clean_val(summary)
            if summary:
                profile_summaries.append(summary)
        if profile_summaries:
            json_data["profile_summaries"] = self.clean_json_dumps(profile_summaries)

        # Ownership details (pipe-delimited) -> payload
        ownership_details = [item.strip() for item in raw_data.get("ownership_details", "").split("|") if item.strip()]
        if ownership_details:
            json_data["ownership_details"] = self.clean_json_dumps(ownership_details)

        # Remarks -> payload
        remarks = [item.strip() for item in raw_data.get("remarks", "").split("|") if item.strip()]
        if remarks:
            json_data["remarks"] = self.clean_json_dumps(remarks)

        # Subject country -> payload stringified list
        subject_country_list = raw_data.get("subject_country", []) or []
        subject_countries = []
        for country in subject_country_list:
            country = self.clean_val(country)
            if country:
                subject_countries.append(country)
        if subject_countries:
            json_data["subject_countries"] = self.clean_json_dumps(subject_countries)

        # Official name, official name local, ISO code, etc. -> root payload scalars
        official_name = self.clean_val(raw_data.get("official_name", ""))
        if official_name:
            json_data["official_name"] = official_name

        official_name_local = self.clean_val(raw_data.get("official_name_in_local_language", ""))
        if official_name_local:
            json_data["official_name_local"] = official_name_local

        iso_code = self.clean_val(raw_data.get("iso_code", ""))
        if iso_code:
            json_data["iso_code"] = iso_code

        # abbreviated_name is a list
        abbreviated_name_list = raw_data.get("abbreviated_name", []) or []
        abbreviated_names = [self.clean_val(n) for n in abbreviated_name_list if self.clean_val(n)]
        if abbreviated_names:
            json_data["abbreviated_names"] = self.clean_json_dumps(abbreviated_names)

        # official_language is a list
        official_language_list = raw_data.get("official_language", []) or []
        official_languages = [self.clean_val(l) for l in official_language_list if self.clean_val(l)]
        if official_languages:
            json_data["official_languages"] = self.clean_json_dumps(official_languages)

        un_lo_code = self.clean_val(raw_data.get("un_locode", ""))
        if un_lo_code:
            json_data["un_locode"] = un_lo_code

        iata_code = self.clean_val(raw_data.get("iata_code", ""))
        if iata_code:
            json_data["iata_code"] = iata_code

        intl_calling_code = self.clean_val(raw_data.get("international_calling_code", ""))
        if intl_calling_code:
            json_data["international_calling_code"] = intl_calling_code

        # FAX -> fix to PHONE_TYPE=FAX, PHONE_NUMBER
        fax_number_list = raw_data.get("fax_number", []) or []
        for fax in fax_number_list:
            fax = self.clean_val(fax)
            if fax:
                json_data["FEATURES"].append({"PHONE_TYPE": "FAX", "PHONE_NUMBER": fax})

        # PEP status details, sanctions status details, etc. -> payload
        pep_status_list = [item.strip() for item in raw_data.get("pep_status", "").split("|") if item.strip()]
        if pep_status_list:
            json_data["pep_status_detail"] = self.clean_json_dumps(pep_status_list)

        pep_remarks = [self.clean_val(r) for r in (raw_data.get("pep_remarks", []) or []) if self.clean_val(r)]
        if pep_remarks:
            json_data["pep_remarks"] = self.clean_json_dumps(pep_remarks)

        sanction_remarks = [
            self.clean_val(r) for r in (raw_data.get("sanctions_remarks", []) or []) if self.clean_val(r)
        ]
        if sanction_remarks:
            json_data["sanction_remarks"] = self.clean_json_dumps(sanction_remarks)

        watchlist_remarks = [
            self.clean_val(r) for r in (raw_data.get("watchlists_remarks", []) or []) if self.clean_val(r)
        ]
        if watchlist_remarks:
            json_data["watchlist_remarks"] = self.clean_json_dumps(watchlist_remarks)

        enforcement_remarks = [
            self.clean_val(r) for r in (raw_data.get("enforcement_remarks", []) or []) if self.clean_val(r)
        ]
        if enforcement_remarks:
            json_data["enforcement_remarks"] = self.clean_json_dumps(enforcement_remarks)

        apc_remarks = [self.clean_val(r) for r in (raw_data.get("apc_remarks", []) or []) if self.clean_val(r)]
        if apc_remarks:
            json_data["apc_remarks"] = self.clean_json_dumps(apc_remarks)

        # Status scalars -> root payload
        sanctions_status = self.clean_val(raw_data.get("sanctions_status", ""))
        if sanctions_status:
            json_data["sanctions_status_detail"] = sanctions_status

        watchlist_status = self.clean_val(raw_data.get("watchlists_status", ""))
        if watchlist_status:
            json_data["watchlist_status_detail"] = watchlist_status

        apc_status = self.clean_val(raw_data.get("apc_status", ""))
        if apc_status:
            json_data["apc_status_detail"] = apc_status

        enforcement_status = self.clean_val(raw_data.get("enforcement_status", ""))
        if enforcement_status:
            json_data["enforcement_status_detail"] = enforcement_status

        # Change category -> root payload
        change_category = self.clean_val(raw_data.get("update_category", ""))
        if change_category:
            json_data["change_category"] = change_category

        # Boolean statuses -> keep at root (correct)
        def str_to_bool(value):
            if isinstance(value, str):
                value = value.lower()
                if value in ("true", "t", "1"):
                    return "True"
                elif value in ("false", "f", "0"):
                    return "False"
            return "True" if bool(value) else "False"

        json_data["PEP_STATUS"] = str_to_bool(raw_data.get("is_pep", False))
        json_data["SANCTION_STATUS"] = str_to_bool(raw_data.get("is_sanction", False))
        json_data["WATCHLIST_STATUS"] = str_to_bool(raw_data.get("is_watchlist", False))
        json_data["ENFORCEMENT_STATUS"] = str_to_bool(raw_data.get("is_enforcement", False))
        json_data["APC_STATUS"] = str_to_bool(raw_data.get("is_apc", False))

        # Sanctions -> payload stringified list
        sanction_authority_list = raw_data.get("sanctions_authority", []) or []
        sanction_authority_country_list = raw_data.get("sanctions_authority_country", []) or []
        sanction_action_date_date_list = raw_data.get("sanctions_action_date_date", []) or []
        sanction_action_date_month_list = raw_data.get("sanctions_action_date_month", []) or []
        sanction_action_date_year_list = raw_data.get("sanctions_action_date_year", []) or []
        sanction_change_date_date_list = raw_data.get("sanctions_change_date_date", []) or []
        sanction_change_date_month_list = raw_data.get("sanctions_change_date_month", []) or []
        sanction_change_date_year_list = raw_data.get("sanctions_change_date_year", []) or []
        sanction_end_date_date_list = raw_data.get("sanctions_end_date_date", []) or []
        sanction_end_date_month_list = raw_data.get("sanctions_end_date_month", []) or []
        sanction_end_date_year_list = raw_data.get("sanctions_end_date_year", []) or []
        sanction_legal_action_type_list = raw_data.get("sanctions_legal_action_type", []) or []
        sanction_order_number_list = raw_data.get("sanctions_order_number", []) or []
        sanction_programme_name_list = raw_data.get("sanctions_programme_name", []) or []
        sanction_programme_country_list = raw_data.get("sanctions_programme_country", []) or []
        sanction_programme_country_code_list = raw_data.get("sanctions_programme_country_code", []) or []
        sanction_authority_ids_list = raw_data.get("sanction_authority_id", []) or []
        sanction_list_names_list = raw_data.get("sanctions_list_name", []) or []

        sanctions = []
        for (
            sanction_authority,
            sanction_authority_country,
            sanction_action_date_date,
            sanction_action_date_month,
            sanction_action_date_year,
            sanction_change_date_date,
            sanction_change_date_month,
            sanction_change_date_year,
            sanction_end_date_date,
            sanction_end_date_month,
            sanction_end_date_year,
            sanction_legal_action_type,
            sanction_order_number,
            sanction_programme_name,
            sanction_programme_country,
            sanction_programme_country_code,
            sanction_authority_id,
            sanction_list_name,
        ) in zip_longest(
            sanction_authority_list,
            sanction_authority_country_list,
            sanction_action_date_date_list,
            sanction_action_date_month_list,
            sanction_action_date_year_list,
            sanction_change_date_date_list,
            sanction_change_date_month_list,
            sanction_change_date_year_list,
            sanction_end_date_date_list,
            sanction_end_date_month_list,
            sanction_end_date_year_list,
            sanction_legal_action_type_list,
            sanction_order_number_list,
            sanction_programme_name_list,
            sanction_programme_country_list,
            sanction_programme_country_code_list,
            sanction_authority_ids_list,
            sanction_list_names_list,
        ):
            sanction_authority = self.clean_val(sanction_authority)
            sanction_authority_country = self.clean_val(sanction_authority_country)
            sanction_action_date_date = self.clean_val(sanction_action_date_date)
            sanction_action_date_month = self.clean_val(sanction_action_date_month)
            sanction_action_date_year = self.clean_val(sanction_action_date_year)
            sanction_change_date_date = self.clean_val(sanction_change_date_date)
            sanction_change_date_month = self.clean_val(sanction_change_date_month)
            sanction_change_date_year = self.clean_val(sanction_change_date_year)
            sanction_end_date_date = self.clean_val(sanction_end_date_date)
            sanction_end_date_month = self.clean_val(sanction_end_date_month)
            sanction_end_date_year = self.clean_val(sanction_end_date_year)
            sanction_legal_action_type = self.clean_val(sanction_legal_action_type)
            sanction_order_number = self.clean_val(sanction_order_number)
            sanction_programme_name = self.clean_val(sanction_programme_name)
            sanction_programme_country = self.clean_val(sanction_programme_country)
            sanction_programme_country_code = self.clean_val(sanction_programme_country_code)
            sanction_authority_id = self.clean_val(sanction_authority_id)
            sanction_list_name = self.clean_val(sanction_list_name)

            _data = {
                "authority": sanction_authority,
                "authority_country": sanction_authority_country,
                "action_date_date": sanction_action_date_date,
                "action_date_month": sanction_action_date_month,
                "action_date_year": sanction_action_date_year,
                "change_date_date": sanction_change_date_date,
                "change_date_month": sanction_change_date_month,
                "change_date_year": sanction_change_date_year,
                "end_date_date": sanction_end_date_date,
                "end_date_month": sanction_end_date_month,
                "end_date_year": sanction_end_date_year,
                "legal_action_type": sanction_legal_action_type,
                "order_number": sanction_order_number,
                "programme_name": sanction_programme_name,
                "programme_country": sanction_programme_country,
                "programme_country_code": sanction_programme_country_code,
                "authority_id": sanction_authority_id,
                "list_name": sanction_list_name,
            }
            if any(_data.values()):
                sanctions.append(_data)

        if sanctions:
            json_data["sanctions"] = self.clean_json_dumps(sanctions)

        # Associated -> payload stringified list
        associated_individual_name_list = raw_data.get("association_associated_individual_name", []) or []
        associated_individual_position_list = raw_data.get("association_associated_individual_position", []) or []
        associated_entities_name_list = raw_data.get("association_associated_entities_name", []) or []

        associated = []
        for (
            associated_individual_name,
            associated_individual_position,
            associated_entities_name,
        ) in zip_longest(
            associated_individual_name_list,
            associated_individual_position_list,
            associated_entities_name_list,
        ):
            associated_individual_name = self.clean_val(associated_individual_name)
            associated_individual_position = self.clean_val(associated_individual_position)
            associated_entities_name = self.clean_val(associated_entities_name)

            _data = {
                "individual_name": associated_individual_name,
                "individual_position": associated_individual_position,
                "entities_name": associated_entities_name,
            }
            if any(_data.values()):
                associated.append(_data)

        if associated:
            json_data["associated"] = self.clean_json_dumps(associated)

        # Restrictions -> payload stringified list
        restrictions_list = raw_data.get("restrictions", []) or []
        restrictions = []
        for restriction in restrictions_list:
            restriction = self.clean_val(restriction)
            if restriction:
                restrictions.append(restriction)
        if restrictions:
            json_data["restrictions"] = self.clean_json_dumps(restrictions)

        # Watchlists -> payload stringified list
        watchlist_authority_list = raw_data.get("watchlists_authority", []) or []
        watchlist_list_name_list = raw_data.get("watchlists_list_name", []) or []
        watchlist_list_abbreviation_list = raw_data.get("watchlists_list_abbreviation", []) or []
        watchlist_authority_country_list = raw_data.get("watchlists_authority_country", []) or []
        watchlist_action_date_date_list = raw_data.get("watchlists_action_date_date", []) or []
        watchlist_action_date_month_list = raw_data.get("watchlists_action_date_month", []) or []
        watchlist_action_date_year_list = raw_data.get("watchlists_action_date_year", []) or []
        watchlist_additional_information_list = raw_data.get("watchlists_additional_information", []) or []
        watchlist_list_id_list = raw_data.get("watchlists_list_id", []) or []

        watchlists = []
        for (
            watchlist_authority,
            watchlist_list_name,
            watchlist_list_abbreviation,
            watchlist_authority_country,
            watchlist_action_date_date,
            watchlist_action_date_month,
            watchlist_action_date_year,
            watchlist_additional_information,
            watchlist_list_id,
        ) in zip_longest(
            watchlist_authority_list,
            watchlist_list_name_list,
            watchlist_list_abbreviation_list,
            watchlist_authority_country_list,
            watchlist_action_date_date_list,
            watchlist_action_date_month_list,
            watchlist_action_date_year_list,
            watchlist_additional_information_list,
            watchlist_list_id_list,
        ):
            watchlist_authority = self.clean_val(watchlist_authority)
            watchlist_list_name = self.clean_val(watchlist_list_name)
            watchlist_list_abbreviation = self.clean_val(watchlist_list_abbreviation)
            watchlist_authority_country = self.clean_val(watchlist_authority_country)
            watchlist_action_date_date = self.clean_val(watchlist_action_date_date)
            watchlist_action_date_month = self.clean_val(watchlist_action_date_month)
            watchlist_action_date_year = self.clean_val(watchlist_action_date_year)
            watchlist_additional_information = self.clean_val(watchlist_additional_information)
            watchlist_list_id = self.clean_val(watchlist_list_id)

            _data = {
                "authority": watchlist_authority,
                "list_name": watchlist_list_name,
                "list_abbreviation": watchlist_list_abbreviation,
                "authority_country": watchlist_authority_country,
                "action_date_date": watchlist_action_date_date,
                "action_date_month": watchlist_action_date_month,
                "action_date_year": watchlist_action_date_year,
                "additional_information": watchlist_additional_information,
                "list_id": watchlist_list_id,
            }
            if any(_data.values()):
                watchlists.append(_data)

        if watchlists:
            json_data["watchlists"] = self.clean_json_dumps(watchlists)

        # Enforcement -> payload stringified list
        enforcement_legal_action_type_list = raw_data.get("enforcement_legal_action_type", []) or []
        enforcement_legal_action_date_day_list = raw_data.get("enforcement_legal_action_date_day", []) or []
        enforcement_legal_action_date_month_list = raw_data.get("enforcement_legal_action_date_month", []) or []
        enforcement_legal_action_date_year_list = raw_data.get("enforcement_legal_action_date_year", []) or []
        enforcement_imprisonment_or_restriction_list = raw_data.get("enforcement_imprisonment_or_restriction", []) or []
        enforcement_fine_amount_in_local_currency_list = (
            raw_data.get("enforcement_fine_amount_in_local_currency", []) or []
        )
        enforcement_name_of_local_currency_list = raw_data.get("enforcement_name_of_local_currency", []) or []
        enforcement_fine_amount_in_usd_list = raw_data.get("enforcement_fine_amount_in_usd", []) or []
        enforcement_conversion_rate_list = raw_data.get("enforcement_conversion_rate", []) or []
        enforcement_primary_regulators_list = raw_data.get("enforcement_primary_regulators", []) or []
        enforcement_stated_regulations_list = raw_data.get("enforcement_stated_regulations", []) or []
        enforcement_enforcement_list_name_list = raw_data.get("enforcement_enforcement_list_name", []) or []
        enforcement_profile_summary_list = raw_data.get("enforcement_profile_summary", []) or []
        enforcement_reasoning_for_legal_actions_list = raw_data.get("enforcement_reasoning_for_legal_actions", []) or []
        enforcement_taxonomy_list = raw_data.get("enforcement_taxonomy", []) or []
        enforcement_event_id_list = raw_data.get("enforcement_event_id", []) or []

        enforcements = []
        for (
            enforcement_legal_action_type,
            enforcement_legal_action_date_day,
            enforcement_legal_action_date_month,
            enforcement_legal_action_date_year,
            enforcement_imprisonment_or_restriction,
            enforcement_fine_amount_in_local_currency,
            enforcement_name_of_local_currency,
            enforcement_fine_amount_in_usd,
            enforcement_conversion_rate,
            enforcement_primary_regulators,
            enforcement_stated_regulations,
            enforcement_enforcement_list_name,
            enforcement_profile_summary,
            enforcement_reasoning_for_legal_actions,
            enforcement_taxonomy,
            enforcement_event_id,
        ) in zip_longest(
            enforcement_legal_action_type_list,
            enforcement_legal_action_date_day_list,
            enforcement_legal_action_date_month_list,
            enforcement_legal_action_date_year_list,
            enforcement_imprisonment_or_restriction_list,
            enforcement_fine_amount_in_local_currency_list,
            enforcement_name_of_local_currency_list,
            enforcement_fine_amount_in_usd_list,
            enforcement_conversion_rate_list,
            enforcement_primary_regulators_list,
            enforcement_stated_regulations_list,
            enforcement_enforcement_list_name_list,
            enforcement_profile_summary_list,
            enforcement_reasoning_for_legal_actions_list,
            enforcement_taxonomy_list,
            enforcement_event_id_list,
        ):
            enforcement_legal_action_type = self.clean_val(enforcement_legal_action_type)
            enforcement_legal_action_date_day = self.clean_val(enforcement_legal_action_date_day)
            enforcement_legal_action_date_month = self.clean_val(enforcement_legal_action_date_month)
            enforcement_legal_action_date_year = self.clean_val(enforcement_legal_action_date_year)
            enforcement_imprisonment_or_restriction = self.clean_val(enforcement_imprisonment_or_restriction)
            enforcement_fine_amount_in_local_currency = self.clean_val(enforcement_fine_amount_in_local_currency)
            enforcement_name_of_local_currency = self.clean_val(enforcement_name_of_local_currency)
            enforcement_fine_amount_in_usd = self.clean_val(enforcement_fine_amount_in_usd)
            enforcement_conversion_rate = self.clean_val(enforcement_conversion_rate)
            enforcement_primary_regulators = self.clean_val(enforcement_primary_regulators)
            enforcement_stated_regulations = self.clean_val(enforcement_stated_regulations)
            enforcement_enforcement_list_name = self.clean_val(enforcement_enforcement_list_name)
            enforcement_profile_summary = self.clean_val(enforcement_profile_summary)
            enforcement_reasoning_for_legal_actions = self.clean_val(enforcement_reasoning_for_legal_actions)
            enforcement_taxonomy = self.clean_val(enforcement_taxonomy)
            enforcement_event_id = self.clean_val(enforcement_event_id)

            _data = {
                "legal_action_type": enforcement_legal_action_type,
                "legal_action_date_day": enforcement_legal_action_date_day,
                "legal_action_date_month": enforcement_legal_action_date_month,
                "legal_action_date_year": enforcement_legal_action_date_year,
                "imprisonment_or_restriction": enforcement_imprisonment_or_restriction,
                "fine_amount_in_local_currency": enforcement_fine_amount_in_local_currency,
                "name_of_local_currency": enforcement_name_of_local_currency,
                "fine_amount_in_usd": enforcement_fine_amount_in_usd,
                "conversion_rate": enforcement_conversion_rate,
                "primary_regulators": enforcement_primary_regulators,
                "stated_regulations": enforcement_stated_regulations,
                "enforcement_list_name": enforcement_enforcement_list_name,
                "profile_summary": enforcement_profile_summary,
                "reasoning_for_legal_actions": enforcement_reasoning_for_legal_actions,
                "taxonomy": enforcement_taxonomy,
                "event_id": enforcement_event_id,
            }
            if any(_data.values()):
                enforcements.append(_data)

        if enforcements:
            json_data["enforcements"] = self.clean_json_dumps(enforcements)

        # APC -> payload stringified list
        apc_group_id_list = raw_data.get("apc_group_id", []) or []
        apc_article_id_list = raw_data.get("apc_article_id", []) or []
        apc_date_published_list = raw_data.get("date_published_date", []) or []
        apc_heading_list = raw_data.get("apc_heading", []) or []
        apc_news_link_list = raw_data.get("apc_news_link", []) or []
        apc_language_list = raw_data.get("apc_language", []) or []
        apc_news_provider_list = raw_data.get("apc_news_provider", []) or []
        apc_sentiment_list = raw_data.get("apc_sentiment", []) or []
        apc_summary_list = raw_data.get("apc_summary", []) or []
        apc_source_reputation_list = raw_data.get("apc_source_reputation", []) or []
        apc_article_text_list = raw_data.get("apc_article_text", []) or []
        apc_summary_lede_list = raw_data.get("apc_summary_lede", []) or []
        apc_month_published_list = raw_data.get("date_published_month", []) or []
        apc_year_published_list = raw_data.get("date_published_year", []) or []
        apc_frameworks_name_list = raw_data.get("apc_frameworks_name", []) or []
        apc_frameworks_version_list = raw_data.get("apc_frameworks_version", []) or []
        apc_risk_score_list = raw_data.get("apc_risk_score", []) or []
        apc_categories_list = raw_data.get("apc_categories", []) or []
        apc_risk_areas_list = raw_data.get("apc_risk_areas", []) or []
        apc_events_list = raw_data.get("apc_events", []) or []
        apc_keywords_list = raw_data.get("apc_keywords", []) or []
        apc_event_stage_list = raw_data.get("apc_event_stage", []) or []
        apc_ner_type_list = raw_data.get("apc_ner_type", []) or []
        apc_ner_entities_list = raw_data.get("apc_ner_entities", []) or []
        apc_ner_attributes_list = raw_data.get("apc_ner_attributes", []) or []
        apc_relevance_score_list = raw_data.get("apc_relevance_score", []) or []
        apc_locations_list = raw_data.get("apc_locations", []) or []
        apc_article_category_list = raw_data.get("apc_article_category", []) or []
        apc_network_map_list = raw_data.get("apc_network_map", []) or []
        apc_risk_event_list = raw_data.get("apc_risk_event", []) or []
        apc_event_chronology_list = raw_data.get("apc_event_chronology", []) or []
        apc_regulatory_action_list = raw_data.get("apc_regulatory_action", []) or []
        apc_regulator_list = raw_data.get("apc_regulator", []) or []
        apc_penalty_amount_list = raw_data.get("apc_penalty_amount", []) or []

        apc_items = []
        for (
            apc_group_id,
            apc_article_id,
            apc_date_published,
            apc_month_published,
            apc_year_published,
            apc_heading,
            apc_news_link,
            apc_language,
            apc_news_provider,
            apc_sentiment,
            apc_summary,
            apc_source_reputation,
            apc_article_text,
            apc_summary_lede,
            apc_frameworks_name,
            apc_frameworks_version,
            apc_risk_score,
            apc_categories,
            apc_risk_areas,
            apc_events,
            apc_keywords,
            apc_event_stage,
            apc_ner_type,
            apc_ner_entities,
            apc_ner_attributes,
            apc_relevance_score,
            apc_locations,
            apc_article_category,
            apc_network_map,
            apc_risk_event,
            apc_event_chronology,
            apc_regulatory_action,
            apc_regulator,
            apc_penalty_amount,
        ) in zip_longest(
            apc_group_id_list,
            apc_article_id_list,
            apc_date_published_list,
            apc_month_published_list,
            apc_year_published_list,
            apc_heading_list,
            apc_news_link_list,
            apc_language_list,
            apc_news_provider_list,
            apc_sentiment_list,
            apc_summary_list,
            apc_source_reputation_list,
            apc_article_text_list,
            apc_summary_lede_list,
            apc_frameworks_name_list,
            apc_frameworks_version_list,
            apc_risk_score_list,
            apc_categories_list,
            apc_risk_areas_list,
            apc_events_list,
            apc_keywords_list,
            apc_event_stage_list,
            apc_ner_type_list,
            apc_ner_entities_list,
            apc_ner_attributes_list,
            apc_relevance_score_list,
            apc_locations_list,
            apc_article_category_list,
            apc_network_map_list,
            apc_risk_event_list,
            apc_event_chronology_list,
            apc_regulatory_action_list,
            apc_regulator_list,
            apc_penalty_amount_list,
        ):
            _data = {
                "group_id": self.clean_val(apc_group_id),
                "article_id": self.clean_val(apc_article_id),
                "date_published": self.clean_val(apc_date_published),
                "month_published": self.clean_val(apc_month_published),
                "year_published": self.clean_val(apc_year_published),
                "heading": self.clean_val(apc_heading),
                "news_link": self.clean_val(apc_news_link),
                "language": self.clean_val(apc_language),
                "news_provider": self.clean_val(apc_news_provider),
                "sentiment": self.clean_val(apc_sentiment),
                "summary": self.clean_val(apc_summary),
                "source_reputation": self.clean_val(apc_source_reputation),
                "article_text": self.clean_val(apc_article_text),
                "summary_lede": self.clean_val(apc_summary_lede),
                "frameworks_name": self.clean_val(apc_frameworks_name),
                "frameworks_version": self.clean_val(apc_frameworks_version),
                "risk_score": self.clean_val(apc_risk_score),
                "categories": self.clean_val(apc_categories),
                "risk_areas": self.clean_val(apc_risk_areas),
                "events": self.clean_val(apc_events),
                "keywords": self.clean_val(apc_keywords),
                "event_stage": self.clean_val(apc_event_stage),
                "ner_type": self.clean_val(apc_ner_type),
                "ner_entities": self.clean_val(apc_ner_entities),
                "ner_attributes": self.clean_val(apc_ner_attributes),
                "relevance_score": self.clean_val(apc_relevance_score),
                "locations": self.clean_val(apc_locations),
                "article_category": self.clean_val(apc_article_category),
                "network_map": self.clean_val(apc_network_map),
                "risk_event": self.clean_val(apc_risk_event),
                "event_chronology": self.clean_val(apc_event_chronology),
                "regulatory_action": self.clean_val(apc_regulatory_action),
                "regulator": self.clean_val(apc_regulator),
                "penalty_amount": self.clean_val(apc_penalty_amount),
            }
            if any(_data.values()):
                apc_items.append(_data)

        if apc_items:
            json_data["apc"] = self.clean_json_dumps(apc_items)

        # Litigation -> payload stringified list
        court_name_list = raw_data.get("litigation_court_name", []) or []
        number_of_cases_list = raw_data.get("litigation_number_of_cases", []) or []
        case_number_list = raw_data.get("litigation_case_number", []) or []
        litigation_date_date_list = raw_data.get("litigation_date_date", []) or []
        litigation_date_month_list = raw_data.get("litigation_date_month", []) or []
        litigation_date_year_list = raw_data.get("litigation_date_year", []) or []

        litigations = []
        for (
            court_name,
            number_of_cases,
            case_number,
            litigation_date_date,
            litigation_date_month,
            litigation_date_year,
        ) in zip_longest(
            court_name_list,
            number_of_cases_list,
            case_number_list,
            litigation_date_date_list,
            litigation_date_month_list,
            litigation_date_year_list,
        ):
            _data = {
                "court_name": self.clean_val(court_name),
                "number_of_cases": self.clean_val(number_of_cases),
                "case_number": self.clean_val(case_number),
                "date_date": self.clean_val(litigation_date_date),
                "date_month": self.clean_val(litigation_date_month),
                "date_year": self.clean_val(litigation_date_year),
            }
            if any(_data.values()):
                litigations.append(_data)

        if litigations:
            json_data["litigations"] = self.clean_json_dumps(litigations)

        # Pincode -> payload stringified list
        pincode_high_risk_area = self.clean_val(raw_data.get("pincode_high_risk_area", ""))
        pincode_risk_type = self.clean_val(raw_data.get("pincode_risk_type", ""))
        pincode_city = self.clean_val(raw_data.get("pincode_city", ""))
        pincode_district = self.clean_val(raw_data.get("pincode_district", ""))
        pincode_state = self.clean_val(raw_data.get("pincode_state", ""))
        pincode_country = self.clean_val(raw_data.get("pincode_country", ""))

        if any(
            [
                pincode_high_risk_area,
                pincode_risk_type,
                pincode_city,
                pincode_district,
                pincode_state,
                pincode_country,
            ]
        ):
            pincode_data = {
                "high_risk_area": pincode_high_risk_area,
                "risk_type": pincode_risk_type,
                "city": pincode_city,
                "district": pincode_district,
                "state": pincode_state,
                "country": pincode_country,
            }
            json_data["pincode"] = self.clean_json_dumps([pincode_data])

        # Others -> payload stringified list
        others_authority_list = raw_data.get("others_authority", []) or []
        others_list_name_list = raw_data.get("others_list_name", []) or []
        others_order_list = raw_data.get("others_order", []) or []
        others_programme_list = raw_data.get("others_programme", []) or []
        others_event_start_date_date_list = raw_data.get("others_event_start_date_date", []) or []
        others_event_start_date_month_list = raw_data.get("others_event_start_date_month", []) or []
        others_event_start_date_year_list = raw_data.get("others_event_start_date_year", []) or []
        others_event_end_date_date_list = raw_data.get("others_event_end_date_date", []) or []
        others_event_end_date_month_list = raw_data.get("others_event_end_date_month", []) or []
        others_event_end_date_year_list = raw_data.get("others_event_end_date_year", []) or []
        others_associated_subject_type_list = raw_data.get("others_associated_subject_type", []) or []
        others_event_summary_list = raw_data.get("others_event_summary", []) or []
        others_reasoning_taxonomy_list = raw_data.get("others_reasoning_taxonomy", []) or []

        others = []
        for (
            others_authority,
            others_list_name,
            others_order,
            others_programme,
            others_event_start_date_date,
            others_event_start_date_month,
            others_event_start_date_year,
            others_event_end_date_date,
            others_event_end_date_month,
            others_event_end_date_year,
            others_associated_subject_type,
            others_event_summary,
            others_reasoning_taxonomy,
        ) in zip_longest(
            others_authority_list,
            others_list_name_list,
            others_order_list,
            others_programme_list,
            others_event_start_date_date_list,
            others_event_start_date_month_list,
            others_event_start_date_year_list,
            others_event_end_date_date_list,
            others_event_end_date_month_list,
            others_event_end_date_year_list,
            others_associated_subject_type_list,
            others_event_summary_list,
            others_reasoning_taxonomy_list,
        ):
            _data = {
                "authority": self.clean_val(others_authority),
                "list_name": self.clean_val(others_list_name),
                "order": self.clean_val(others_order),
                "programme": self.clean_val(others_programme),
                "event_start_date_date": self.clean_val(others_event_start_date_date),
                "event_start_date_month": self.clean_val(others_event_start_date_month),
                "event_start_date_year": self.clean_val(others_event_start_date_year),
                "event_end_date_date": self.clean_val(others_event_end_date_date),
                "event_end_date_month": self.clean_val(others_event_end_date_month),
                "event_end_date_year": self.clean_val(others_event_end_date_year),
                "associated_subject_type": self.clean_val(others_associated_subject_type),
                "event_summary": self.clean_val(others_event_summary),
                "reasoning_taxonomy": self.clean_val(others_reasoning_taxonomy),
            }
            if any(_data.values()):
                others.append(_data)

        if others:
            json_data["others"] = self.clean_json_dumps(others)

        # Remove empty dictionaries or dictionaries with only empty values
        json_data["FEATURES"] = [
            item
            for item in json_data["FEATURES"]
            if any(value for value in item.values() if value not in [None, "", [], {}])
        ]

        # --remove empty attributes and capture the stats
        json_data = self.remove_empty_tags(json_data)
        self.capture_mapped_stats(json_data)

        # Reorder: DATA_SOURCE, RECORD_ID, FEATURES, then payload
        ordered = {}
        ordered["DATA_SOURCE"] = json_data.pop("DATA_SOURCE", "")
        ordered["RECORD_ID"] = json_data.pop("RECORD_ID", "")
        ordered["FEATURES"] = json_data.pop("FEATURES", [])
        ordered.update(json_data)

        return ordered

    # ----------------------------------------
    def load_reference_data(self):

        # --garabage values
        self.variant_data = {}
        self.variant_data["GARBAGE_VALUES"] = ["NULL", "NUL", "N/A", "~"]

    # -----------------------------------
    def clean_value(self, raw_value):
        if raw_value is None:
            return ""
        if isinstance(raw_value, list):
            # clean each element in the list
            return [self.clean_value(x) for x in raw_value]
        # If it's a single value, clean as before
        new_value = " ".join(str(raw_value).strip().split())
        if new_value.upper() in self.variant_data["GARBAGE_VALUES"]:
            return ""
        return new_value

    # -----------------------------------
    def compute_record_hash(self, target_dict, attr_list=None):
        if attr_list:
            string_to_hash = ""
            for attr_name in sorted(attr_list):
                string_to_hash += (
                    " ".join(str(target_dict[attr_name]).split()).upper()
                    if attr_name in target_dict and target_dict[attr_name]
                    else ""
                ) + "|"
        else:
            string_to_hash = json.dumps(target_dict, sort_keys=True)
        return hashlib.md5(bytes(string_to_hash, "utf-8")).hexdigest()

    # ----------------------------------------
    def format_date(self, raw_date):
        try:
            return datetime.strftime(dateparse(raw_date), "%Y-%m-%d")
        except:
            self.update_stat("!INFO", "BAD_DATE", raw_date)
            return ""

    # ----------------------------------------
    def remove_empty_tags(self, d):
        if isinstance(d, dict):
            for k, v in list(d.items()):
                if v is None or len(str(v).strip()) == 0:
                    del d[k]
                else:
                    self.remove_empty_tags(v)
        if isinstance(d, list):
            for v in d:
                self.remove_empty_tags(v)
        return d

    def clean_json_dumps(self, data):
        """json.dumps that strips blank/None values from dicts first."""
        self.remove_empty_tags(data)
        return json.dumps(data)

    # ----------------------------------------
    def update_stat(self, cat1, cat2, example=None):

        if cat1 not in self.stat_pack:
            self.stat_pack[cat1] = {}
        if cat2 not in self.stat_pack[cat1]:
            self.stat_pack[cat1][cat2] = {}
            self.stat_pack[cat1][cat2]["count"] = 0

        self.stat_pack[cat1][cat2]["count"] += 1
        if example:
            if "examples" not in self.stat_pack[cat1][cat2]:
                self.stat_pack[cat1][cat2]["examples"] = []
            if example not in self.stat_pack[cat1][cat2]["examples"]:
                if len(self.stat_pack[cat1][cat2]["examples"]) < 5:
                    self.stat_pack[cat1][cat2]["examples"].append(example)
                else:
                    randomSampleI = random.randint(2, 4)
                    self.stat_pack[cat1][cat2]["examples"][randomSampleI] = example
        return

    # ----------------------------------------
    def capture_mapped_stats(self, json_data):

        if "DATA_SOURCE" in json_data:
            data_source = json_data["DATA_SOURCE"]
        else:
            data_source = "UNKNOWN_DSRC"

        for key1 in json_data:
            if type(json_data[key1]) != list:
                self.update_stat(data_source, key1, json_data[key1])
            else:
                for subrecord in json_data[key1]:
                    for key2 in subrecord:
                        self.update_stat(data_source, key2, subrecord[key2])

    # ----------------------------------------
    def clean_val(self, value):
        try:
            if isinstance(value, list):
                if len(value) == 1:
                    value = value[0]
                elif len(value) == 0:
                    return ""  # empty list -> blank
            if value is None:
                return ""
            value = str(value).strip()
            return "" if value == "~" else value
        except Exception:
            return ""


# ----------------------------------------
def signal_handler(signal, frame):
    print("USER INTERUPT! Shutting down ... (please wait)")
    global shut_down
    shut_down = True
    return


# ----------------------------------------
if __name__ == "__main__":
    proc_start_time = time.time()
    shut_down = False
    signal.signal(signal.SIGINT, signal_handler)

    parser = argparse.ArgumentParser()
    parser.add_argument("-i", "--input_file", dest="input_file", help="the name of the input file")
    parser.add_argument("-o", "--output_file", dest="output_file", help="the name of the output file")
    parser.add_argument(
        "-l",
        "--log_file",
        dest="log_file",
        help="optional name of the statistics log file",
    )
    parser.add_argument("-d", "--data_source", dest="data_source", help="data source code (required)")
    args = parser.parse_args()

    if not args.input_file or not os.path.exists(args.input_file):
        print("\nPlease supply a valid input file name on the command line\n")
        sys.exit(1)
    if not args.output_file:
        print("\nPlease supply a valid output file name on the command line\n")
        sys.exit(1)
    if not args.data_source:
        print("\nPlease supply a data source code on the command line\n")
        sys.exit(1)

    input_file_handle = open(args.input_file, "r", encoding="utf-8")
    output_file_handle = open(args.output_file, "w", encoding="utf-8")

    mapper_obj = mapper()  # renamed to avoid shadowing the class/function

    input_row_count = 0
    output_row_count = 0

    for line in input_file_handle:
        input_row_count += 1
        try:
            input_row = json.loads(line)
        except json.JSONDecodeError as e:
            print(f"Skipping line {input_row_count} due to JSON parse error: {e}")
            continue

        json_data = mapper_obj.map(input_row, input_row_count)
        if json_data:
            output_file_handle.write(json.dumps(json_data) + "\n")
            output_row_count += 1

        if input_row_count % 1000 == 0:
            print(f"{input_row_count} rows processed, {output_row_count} rows written")
        if shut_down:
            break

    elapsed_mins = round((time.time() - proc_start_time) / 60, 1)
    run_status = ("completed in" if not shut_down else "aborted after") + f" {elapsed_mins} minutes"
    print(f"{input_row_count} rows processed, {output_row_count} rows written, {run_status}\n")

    output_file_handle.close()
    input_file_handle.close()

    sys.exit(0)
