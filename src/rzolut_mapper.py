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

import pandas as pd
from dateutil.parser import parse as dateparse


# =========================
class mapper:

    # ----------------------------------------
    def __init__(self):

        self.load_reference_data()
        self.stat_pack = {}

    # ----------------------------------------
    def map(self, raw_data, input_row_num=None):
        json_data = {}

        # Clean the raw data values using the clean_value method
        for attribute in raw_data:
            raw_data[attribute] = self.clean_value(raw_data[attribute])

        json_data["FEATURES"] = (
            []
        )  # Initialize the FEATURES list to hold additional attributes

        # Set essential fields for the JSON data
        json_data["RECORD_ID"] = raw_data["uid"]  # Unique identifier for the record
        json_data["DATA_SOURCE"] = args.data_source  # Source of the data

        # Record type is optional, but should be 'PERSON' or 'ORGANIZATION'
        self.update_stat(
            raw_data.get("subject_type", "").upper(), raw_data["uid"]
        )  # Update statistics based on type
        json_data["RECORD_TYPE"] = (
            "PERSON"
            if raw_data.get("subject_type", "") == "Individual"
            else "ORGANIZATION"
        )
        json_data["TYPE"] = raw_data.get(
            "subject_type"
        )  # Store the type from raw_data, if available

        # Set primary names
        json_data["PRIMARY_NAME_FIRST"] = raw_data.get("first_name")  # First name
        json_data["PRIMARY_NAME_MIDDLE"] = raw_data.get("middle_name")  # Middle name
        json_data["PRIMARY_NAME_LAST"] = raw_data.get("last_name")  # Last name

        # Primary name of the organization
        json_data["PRIMARY_NAME_ORG"] = raw_data.get("name")

        # Append gender information if available
        if raw_data.get("gender", ""):
            json_data["FEATURES"].append({"GENDER": raw_data.get("gender", "")})

        # Process image URLs, split by '|', and add to FEATURES
        if raw_data.get("image_url"):
            img = raw_data.get("image_url", "")
            if img.strip():  # Ensure the image URL is not empty
                json_data["FEATURES"].append({"image_url": img.strip()})

        # Process date of birth information
        date_of_birth_year_list = raw_data.get("date_of_birth_year", []) or []
        date_of_birth_month_list = raw_data.get("date_of_birth_month", []) or []
        date_of_birth_date_list = raw_data.get("date_of_birth_date", []) or []

        # Loop through each component of the date of birth
        for year, month, date in zip_longest(
            date_of_birth_year_list,
            date_of_birth_month_list,
            date_of_birth_date_list,
            fillvalue="",
        ):
            try:
                y = self.clean_val(year)  # Year
                m = self.clean_val(month)  # Month
                d = self.clean_val(date)  # Day

                # Construct date of birth based on available components
                if y and m and d:
                    json_data["FEATURES"].append({"DATE_OF_BIRTH": f"{y}-{m}-{d}"})
                elif y and m:
                    json_data["FEATURES"].append({"DATE_OF_BIRTH": f"{y}-{m}"})
                elif m and d:
                    json_data["FEATURES"].append({"DATE_OF_BIRTH": f"{m}/{d}"})
                elif y:
                    json_data["FEATURES"].append({"DATE_OF_BIRTH": y})
            except Exception as ex:
                print(
                    f"id {raw_data['uid']} date_of_birth parse error {ex}"
                )  # Log parsing errors

        # Process date of death information
        date_of_death_year_list = raw_data.get("date_of_death_year", []) or []
        date_of_death_month_list = raw_data.get("date_of_death_month", []) or []
        date_of_death_date_list = raw_data.get("date_of_death_date", []) or []
        is_deceased = raw_data.get(
            "deceased_status", ""
        )  # Check if the individual is deceased
        json_data["country"] = is_deceased  # Store deceased status in 'country' field

        if is_deceased:
            # Loop through each component of the date of death
            for year, month, date in zip_longest(
                date_of_death_year_list,
                date_of_death_month_list,
                date_of_death_date_list,
                fillvalue="",
            ):
                try:
                    y = self.clean_val(year)  # Year
                    m = self.clean_val(month)  # Month
                    d = self.clean_val(date)  # Day

                    # Construct date of death based on available components
                    if y and m and d:
                        json_data["FEATURES"].append({"DATE_OF_DEATH": f"{y}-{m}-{d}"})
                    elif y and m:
                        json_data["FEATURES"].append({"DATE_OF_DEATH": f"{y}-{m}"})
                    elif m and d:
                        json_data["FEATURES"].append({"DATE_OF_DEATH": f"{m}/{d}"})
                    elif y:
                        json_data["FEATURES"].append({"DATE_OF_DEATH": y})
                except Exception as ex:
                    print(
                        f"id {raw_data['uid']} date_of_death parse error {ex}"
                    )  # Log parsing errors

        # Retrieve and split address-related data into lists
        address_type_list = raw_data.get("address_type", []) or []
        address_street_list = raw_data.get("address_street", []) or []
        address_city_list = raw_data.get("address_city", []) or []
        address_province_list = raw_data.get("address_province", []) or []
        address_postal_code_list = raw_data.get("address_postal_code", []) or []
        address_country_list = raw_data.get("address_country", []) or []
        address_country_code_list = raw_data.get("address_country_code", []) or []

        # Iterate through each address entry to construct address data
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
                _data = {
                    "ADDR_TYPE": self.clean_val(addr_type),
                    "ADDR_LINE1": self.clean_val(street),
                    "ADDR_LINE2": self.clean_val(
                        country
                    ),  # <-- you may want to change this to proper ADDRESS_LINE2
                    "ADDR_CITY": self.clean_val(city),
                    "ADDR_STATE": self.clean_val(province),
                    "ADDR_POSTAL_CODE": self.clean_val(postal),
                    "ADDR_COUNTRY": self.clean_val(country_code),
                }

                # Append only if at least one field is present
                if any(v for v in _data.values()):
                    json_data["FEATURES"].append(_data)

            except Exception as ex:
                print(f"id {raw_data.get('uid')} address parse error {ex}")

        # Set SOE status based on raw data
        json_data["soe_status"] = (
            "Yes" if "Yes" in raw_data.get("soe_status", "") else ""
        )

        # Retrieve and split PEP-related data into lists
        pep_type_list = raw_data.get("pep_type", []) or []
        pep_level_list = raw_data.get("pep_level", []) or []
        position_list = raw_data.get("position", []) or []
        org_name_list = raw_data.get("organization_name", []) or []

        # Retrieve and split position date-related data into lists
        # Parse position dates as lists
        position_start_date_year_list = (
            raw_data.get("position_start_date_year", []) or []
        )
        position_start_date_month_list = (
            raw_data.get("position_start_date_month", []) or []
        )
        position_start_date_date_list = (
            raw_data.get("position_start_date_date", []) or []
        )

        position_end_date_year_list = raw_data.get("position_end_date_year", []) or []
        position_end_date_month_list = raw_data.get("position_end_date_month", []) or []
        position_end_date_date_list = raw_data.get("position_end_date_date", []) or []

        # Add group associations to json_data from organization names
        for name in org_name_list:
            if (
                name.strip() and name != "~"
            ):  # Check if the organization name is not empty
                json_data["FEATURES"].append(
                    {"GROUP_ASSOCIATION_ORG_NAME": name.strip()}
                )

        # Iterate through positions and create data for each
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
                    json_data["FEATURES"].append(_data)

            except Exception as ex:
                print(
                    f"id {raw_data.get('uid', 'unknown')} pep details parse error {ex}"
                )

        # Check if 'alias_name' exists in the raw data
        if raw_data.get("alias_name"):
            # Split alias-related data into lists
            alias_names = raw_data.get("alias_name", []) or []
            alias_types = raw_data.get("alias_type", []) or []
            alias_scripts = raw_data.get("alias_script", []) or []
            alias_languages = raw_data.get("alias_language", []) or []

            # Iterate through the alias lists simultaneously
            for alias_name, alias_type, alias_script, alias_language in zip_longest(
                alias_names, alias_types, alias_scripts, alias_languages
            ):
                alias_name = self.clean_val(alias_name)
                alias_type = self.clean_val(alias_type)
                alias_script = self.clean_val(alias_script)
                alias_language = self.clean_val(alias_language)

                if alias_name:  # Ensure alias_name is not empty
                    if json_data["RECORD_TYPE"] == "PERSON":
                        # Append person-specific alias data
                        json_data["FEATURES"].append(
                            {
                                "ALIAS_NAME_FULL": alias_name,
                                "ALIAS_TYPE": alias_type,
                                "ALIAS_SCRIPT": alias_script,
                                "ALIAS_LANGUAGE": alias_language,
                            }
                        )
                    else:
                        json_data["FEATURES"].append(
                            {
                                "ALIAS_NAME_ORG": alias_name,
                                "ALIAS_TYPE": alias_type,
                                "ALIAS_SCRIPT": alias_script,
                                "ALIAS_LANGUAGE": alias_language,
                            }
                        )

        # Retrieve relationship-related data
        relationship_subject_type_list = (
            raw_data.get("association_subject_type", []) or []
        )
        relationship_name_list = raw_data.get("association_name", []) or []
        relationship_type_list = raw_data.get("association_relationship_type", []) or []
        relationship_type_desc_list = (
            raw_data.get("association_relationship_type_description", []) or []
        )
        relationship_uid_list = raw_data.get("association_relationship_uid", []) or []

        # Append relationship details to json_data
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

                # Always append the base relationship record
                json_data["FEATURES"].append(
                    {
                        "RELATIONSHIP_SUBJECT_TYPE": rel_subject_type,
                        "RELATIONSHIP_NAME": rel_name,
                        "RELATIONSHIP_TYPE": rel_type,
                        "RELATIONSHIP_TYPE_DESCRIPTION": rel_type_desc,
                        "RELATIONSHIP_UID": rel_uid,
                    }
                )

                # Append relationship pointers if we have a UID
                if rel_uid:
                    json_data["FEATURES"].append({"REL_POINTER_KEY": rel_uid})
                    json_data["FEATURES"].append(
                        {"REL_ANCHOR_DOMAIN": args.data_source + "_UID"}
                    )
                    json_data["FEATURES"].append({"REL_ANCHOR_KEY": raw_data["uid"]})
                    if rel_type:
                        json_data["FEATURES"].append({"REL_POINTER_ROLE": rel_type})

            except Exception as ex:
                print(f"id {raw_data.get('uid')} relationship parse error {ex}")

        # Retrieve pep-country data as lists
        pep_country_list = raw_data.get("pep_country", []) or []
        pep_country_code_list = raw_data.get("pep_country_code", []) or []

        # Append source details to json_data
        for pep_country, pep_country_code in zip_longest(
            pep_country_list, pep_country_code_list
        ):
            pep_country = self.clean_val(pep_country)
            pep_country_code = self.clean_val(pep_country_code)

            json_data["FEATURES"].append(
                {"PEP_COUNTRY": pep_country, "PEP_COUNTRY_CODE": pep_country_code}
            )

        # Retrieve source-related data as lists
        source_type_list = raw_data.get("source_type", []) or []
        source_list = raw_data.get("external_sources", []) or []
        source_description_list = raw_data.get("source_description", []) or []

        # Append source details to json_data
        for source_type, source, source_description in zip_longest(
            source_type_list, source_list, source_description_list
        ):
            source_type = self.clean_val(source_type)
            source = self.clean_val(source)
            source_description = self.clean_val(source_description)

            json_data["FEATURES"].append(
                {
                    "SOURCE_TYPE": source_type,
                    "SOURCE": source,
                    "SOURCE_DESCRIPTION": source_description,
                }
            )

        # Add timestamps to json_data
        json_data["CREATED_AT"] = raw_data["entered"]
        json_data["UPDATED_AT"] = raw_data["updated"]

        # Process citizenship data as lists
        citizenship_country_list = raw_data.get("citizenship", []) or []
        citizenship_country_code_list = (
            raw_data.get("citizenship_country_code", []) or []
        )

        for citizenship_country, citizenship_country_code in zip_longest(
            citizenship_country_list, citizenship_country_code_list
        ):
            citizenship_country = self.clean_val(citizenship_country)
            citizenship_country_code = self.clean_val(citizenship_country_code)

            json_data["FEATURES"].append(
                {
                    "CITIZENSHIP": citizenship_country,
                    "CITIZENSHIP_COUNTRY_CODE": citizenship_country_code,
                }
            )

        # Process nationality data as lists
        nationality_country_list = raw_data.get("nationality_country", []) or []
        nationality_country_code_list = (
            raw_data.get("nationality_country_code", []) or []
        )

        for nationality_country, nationality_country_code in zip_longest(
            nationality_country_list, nationality_country_code_list
        ):
            nationality_country = self.clean_val(nationality_country)
            nationality_country_code = self.clean_val(nationality_country_code)

            json_data["FEATURES"].append(
                {
                    "NATIONALITY": nationality_country,
                    "NATIONALITY_COUNTRY_CODE": nationality_country_code,
                }
            )

        # Process identifiers
        # Parse identifier lists as proper Python lists
        identifier_name_list = raw_data.get("identifier_name", []) or []
        identifier_value_list = raw_data.get("identifier_value", []) or []
        identifier_country_list = raw_data.get("identifier_country", []) or []
        identifier_country_code_list = raw_data.get("identifier_country_code", []) or []
        identifier_issuing_authority_list = (
            raw_data.get("identifier_issuing_authority", []) or []
        )

        identifier_issue_date_date_list = (
            raw_data.get("identifier_issue_date_date", []) or []
        )
        identifier_issue_date_month_list = (
            raw_data.get("identifier_issue_date_month", []) or []
        )
        identifier_issue_date_year_list = (
            raw_data.get("identifier_issue_date_year", []) or []
        )

        identifier_expiry_date_date_list = (
            raw_data.get("identifier_expiry_date_date", []) or []
        )
        identifier_expiry_date_month_list = (
            raw_data.get("identifier_expiry_date_month", []) or []
        )
        identifier_expiry_date_year_list = (
            raw_data.get("identifier_expiry_date_year", []) or []
        )

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
                    y = self.clean_val(issue_y)  # Year
                    m = self.clean_val(issue_m)  # Month
                    d = self.clean_val(issue_d)  # Day

                    # Construct date of birth based on available components
                    if y and m and d:
                        identifier_issue_date = f"{y}-{m}-{d}"
                    elif y and m:
                        identifier_issue_date = f"{y}-{m}"
                    elif m and d:
                        identifier_issue_date = f"{m}/{d}"
                    elif y:
                        identifier_issue_date = y
                except Exception as ex:
                    print(
                        f"id {raw_data['uid']} identifier_issue_date parse error {ex}"
                    )  # Log parsing errors

                try:
                    y = self.clean_val(expiry_y)  # Year
                    m = self.clean_val(expiry_m)  # Month
                    d = self.clean_val(expiry_d)  # Day

                    # Construct date of birth based on available components
                    if y and m and d:
                        identifier_expiry_date = f"{y}-{m}-{d}"
                    elif y and m:
                        identifier_expiry_date = f"{y}-{m}"
                    elif m and d:
                        identifier_expiry_date = f"{m}/{d}"
                    elif y:
                        identifier_expiry_date = y
                except Exception as ex:
                    print(
                        f"id {raw_data['uid']} identifier_expiry_date parse error {ex}"
                    )  # Log parsing errors

                # Update statistics for identifier type
                self.update_stat("!IDTYPE", raw_type, value)

                if raw_type == "LEGAL ENTITY IDENTIFIER (LEI)":
                    json_data["FEATURES"].append(
                        {
                            "LEI_NUMBER": value
                        }
                    )

                # Append identifier details based on type
                if raw_type == "PASSPORT NUMBER":
                    json_data["FEATURES"].append(
                        {
                            "PASSPORT_NUMBER": value,
                            "PASSPORT_COUNTRY": country_code,
                            "PASSPORT_ISSUE_DT": identifier_issue_date,
                            "PASSPORT_EXPIRE_DT": identifier_expiry_date,
                        }
                    )
                elif raw_type == "DIRECTOR IDENTIFICATION NUMBER (DIN)":
                    json_data["FEATURES"].append(
                        {"NATIONAL_ID_TYPE": "DIN", "NATIONAL_ID_NUMBER": value, "NATIONAL_ID_COUNTRY": country_code}
                    )
                elif raw_type == "PERMANENT ACCOUNT NUMBER (PAN)":
                    json_data["FEATURES"].append(
                        {
                            "TAX_ID_TYPE": "PAN",
                            "TAX_ID_NUMBER": value,
                            "TAX_ID_COUNTRY": country_code,
                            "TAX_ID_ISSUE_DT": identifier_issue_date,
                            "TAX_ID_EXPIRE_DT": identifier_expiry_date,
                        }
                    )
                elif raw_type == "LICENSE NUMBER":
                    json_data["FEATURES"].append(
                        {
                            "OTHER_ID_TYPE": "LICENSE",
                            "OTHER_ID_NUMBER": value,
                            "OTHER_ID_COUNTRY": country_code,
                            "OTHER_ID_ISSUE_DT": identifier_issue_date,
                            "OTHER_ID_EXPIRE_DT": identifier_expiry_date,
                        }
                    )
                elif raw_type == "CORPORATE IDENTIFICATION NUMBER (CIN)":
                    json_data["FEATURES"].append(
                        {"NATIONAL_ID_TYPE": "CIN", "NATIONAL_ID_NUMBER": value, "NATIONAL_ID_COUNTRY": country_code}
                    )
                elif raw_type == "LIMITED LIABILITY PARTNERSHIP IDENTIFICATION NUMBER (LLPIN)":
                    json_data["FEATURES"].append(
                        {"NATIONAL_ID_TYPE": "CIN", "NATIONAL_ID_NUMBER": value, "NATIONAL_ID_COUNTRY": country_code}
                    )
                elif raw_type == "FCRN NUMBER":
                    json_data["FEATURES"].append(
                        {"NATIONAL_ID_TYPE": "FCRN", "NATIONAL_ID_NUMBER": value, "NATIONAL_ID_COUNTRY": country_code}
                    )
                elif raw_type == "FIRM REGISTRATION NUMBER (FRN)":
                    json_data["FEATURES"].append(
                        {"NATIONAL_ID_TYPE": "FRN", "NATIONAL_ID_NUMBER": value, "NATIONAL_ID_COUNTRY": country_code}
                    )
                elif raw_type == "CADASTRO NACIONAL DA PESSOA JURÍDICA (CNPJ)":
                    json_data["FEATURES"].append(
                        {
                            "TAX_ID_TYPE": "CNPJ",
                            "TAX_ID_NUMBER": value,
                            "TAX_ID_COUNTRY": country_code
                        }
                    )
                elif raw_type == "CADASTRO DE PESSOAS FÍSICAS (CPF)":
                    json_data["FEATURES"].append(
                        {
                            "OTHER_ID_TYPE": "CPF",
                            "OTHER_ID_NUMBER": value,
                            "OTHER_ID_COUNTRY": country_code,
                            "OTHER_ID_ISSUE_DT": identifier_issue_date,
                            "OTHER_ID_EXPIRE_DT": identifier_expiry_date,
                        }
                    )
                else:
                    json_data["FEATURES"].append(
                        {
                            "OTHER_ID_TYPE": raw_type,
                            "OTHER_ID_NUMBER": value,
                            "OTHER_ID_COUNTRY": country_code,
                            "OTHER_ID_ISSUE_DT": identifier_issue_date,
                            "OTHER_ID_EXPIRE_DT": identifier_expiry_date,
                        }
                    )
            except Exception as ex:
                print(
                    f"id {raw_data['uid']} identifier parse error {ex}"
                )  # Log any parsing errors

        # Process vessel information as lists
        vessel_type_list = raw_data.get("vessel_type", []) or []
        current_country_flag_list = raw_data.get("current_country_flag", []) or []
        former_country_flag_list = raw_data.get("former_country_flag", []) or []

        # Compile vessel data into features
        for vessel_type, curr_country, form_country in zip_longest(
            vessel_type_list, current_country_flag_list, former_country_flag_list
        ):
            vessel_type = self.clean_val(vessel_type)
            curr_country = self.clean_val(curr_country)
            form_country = self.clean_val(form_country)

            json_data["FEATURES"].append(
                {
                    "VESSEL_TYPE": vessel_type,
                    "VESSEL_CURRENT_COUNTRY": curr_country,
                    "VESSEL_FORMER_COUNTRY": form_country,
                }
            )

        # Process aircraft information
        # Process aircraft information as lists
        aircraft_manufacture_date_date_list = (
            raw_data.get("aircraft_manufacture_date_date", []) or []
        )
        aircraft_manufacture_date_month_list = (
            raw_data.get("aircraft_manufacture_date_month", []) or []
        )
        aircraft_manufacture_date_year_list = (
            raw_data.get("aircraft_manufacture_date_year", []) or []
        )
        aircraft_model = raw_data.get("aircraft_model", "")

        # Compile aircraft data into features
        for d, m, y, model in zip_longest(
            aircraft_manufacture_date_date_list,
            aircraft_manufacture_date_month_list,
            aircraft_manufacture_date_year_list,
            aircraft_model,
            fillvalue="",
        ):
            json_data["FEATURES"].append(
                {
                    "AIRCRAFT_MANUFACTURE_DATE": self.clean_val(d),
                    "AIRCRAFT_MANUFACTURE_MONTH": self.clean_val(m),
                    "AIRCRAFT_MANUFACTURE_YEAR": self.clean_val(y),
                    "AIRCRAFT_MODEL": self.clean_val(model),
                }
            )

        # Extract incorporation date information
        date_of_incorporation_year_list = (
            raw_data.get("date_of_incorporation_year", []) or []
        )
        date_of_incorporation_month_list = (
            raw_data.get("date_of_incorporation_month", []) or []
        )
        date_of_incorporation_date_list = (
            raw_data.get("date_of_incorporation_date", []) or []
        )

        # Loop through incorporation date lists using zip_longest
        for year, month, date in zip_longest(
            date_of_incorporation_year_list,
            date_of_incorporation_month_list,
            date_of_incorporation_date_list,
            fillvalue="",
        ):
            try:
                y = self.clean_val(year)  # Year
                m = self.clean_val(month)  # Month
                d = self.clean_val(date)  # Day
                # Append the formatted registration date based on available components
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

        # Extract and append country of incorporation to json_data['FEATURES']
        country_code_of_incorporation_list = (
            raw_data.get("country_code_of_incorporation", []) or []
        )
        for incorporation_code in country_code_of_incorporation_list:
            incorporation_code = self.clean_val(incorporation_code)
            if incorporation_code:
                json_data["FEATURES"].append(
                    {"REGISTRATION_COUNTRY": incorporation_code}
                )

        # Extract and append country of origin to json_data['FEATURES']
        country_code_of_origin_list = raw_data.get("country_code_of_origin", []) or []
        for origin_code in country_code_of_origin_list:
            origin_code = self.clean_val(origin_code)
            if origin_code:
                json_data["FEATURES"].append({"COUNTRY": origin_code})

        # Extract and append ownership details to json_data['FEATURES']
        percentage_of_shareholding_list = (
            raw_data.get("association_percentage_of_shareholding", []) or []
        )
        for shareholding in percentage_of_shareholding_list:
            shareholding = self.clean_val(shareholding)
            if shareholding:
                json_data["FEATURES"].append({"OWNERSHIP_DETAILS": shareholding})

        # Process age information
        age_in_yrs_list = raw_data.get("age", []) or []
        for age in age_in_yrs_list:
            age = self.clean_val(age)
            if age:
                json_data["FEATURES"].append({"AGE_BRACKET": age})

        # Process contact numbers
        contact_number_list = raw_data.get("contact_number", []) or []
        for phone in contact_number_list:
            phone = self.clean_val(phone)
            if phone:
                json_data["FEATURES"].append({"PHONE_NUMBER": phone})

        # Process email addresses
        email_id_list = raw_data.get("email_id", []) or []
        for email in email_id_list:
            email = self.clean_val(email)
            if email:
                json_data["FEATURES"].append({"EMAIL_ADDRESS": email})

        # Process website addresses
        website_list = raw_data.get("website", []) or []
        for site in website_list:
            site = self.clean_val(site)
            if site:
                json_data["FEATURES"].append({"WEBSITE_ADDRESS": site})

        # Process hair color information
        color_of_hair_list = raw_data.get("color_of_hair", []) or []
        for hair_color in color_of_hair_list:
            hair_color = self.clean_val(hair_color)
            if hair_color:
                json_data["FEATURES"].append({"COLOR_HAIR": hair_color})

        # Process eye color information
        color_of_eyes_list = raw_data.get("color_of_eyes", []) or []
        for eye_color in color_of_eyes_list:
            eye_color = self.clean_val(eye_color)
            if eye_color:
                json_data["FEATURES"].append({"COLOR_EYES": eye_color})

        # Process height information
        height_list = raw_data.get("height", []) or []
        for height in height_list:
            height = self.clean_val(height)
            if height:
                json_data["FEATURES"].append({"HEIGHT": height})

        # Process weight information
        weight_list = raw_data.get("weight", []) or []
        for weight in weight_list:
            weight = self.clean_val(weight)
            if weight:
                json_data["FEATURES"].append({"WEIGHT": weight})

        # Process distinguishing marks and characteristics
        distinguishing_marks_list = (
            raw_data.get("distinguishing_marks_and_characteristics", []) or []
        )
        for mark in distinguishing_marks_list:
            mark = self.clean_val(mark)
            if mark:
                json_data["FEATURES"].append({"DISTINGUISHING_MARKS": mark})

        # Process profile summaries
        profile_summary_list = raw_data.get("profile_summary", []) or []
        for summary in profile_summary_list:
            summary = self.clean_val(summary)
            if summary:
                json_data["FEATURES"].append({"PROFILE_SUMMARY": summary})

        # Process ownership details
        for item in raw_data.get("ownership_details", "").split("|"):
            if item.strip():
                json_data["FEATURES"].append({"OWNERSHIP_DETAILS": item.strip()})

        # Process remarks
        for item in raw_data.get("remarks", "").split("|"):
            if item.strip():
                json_data["FEATURES"].append({"REMARKS": item.strip()})

        # Process subject country
        subject_country_list = raw_data.get("subject_country", []) or []
        for country in subject_country_list:
            country = self.clean_val(country)
            if country:
                json_data["FEATURES"].append({"SUBJECT_COUNTRY": country})

        # Process official name
        official_name = self.clean_val(raw_data.get("official_name", ""))
        if official_name:
            json_data["FEATURES"].append({"OFFICIAL_NAME": official_name})

        # Process official name in local language
        official_name_local = self.clean_val(
            raw_data.get("official_name_in_local_language", "")
        )
        if official_name_local:
            json_data["FEATURES"].append(
                {"OFFICIAL_NAME_IN_LOCAL_LANGUAGE": official_name_local}
            )

        # Process ISO code
        iso_code = self.clean_val(raw_data.get("iso_code", ""))
        if iso_code:
            json_data["FEATURES"].append({"ISO_CODE": iso_code})

        # Process abbreviated name
        abbreviated_name_list = raw_data.get("abbreviated_name", []) or []
        for name in abbreviated_name_list:
            name = self.clean_val(name)
            if name:
                json_data["FEATURES"].append({"ABBREVIATED_NAME": name})

        # Process official language
        official_language_list = raw_data.get("official_language", []) or []
        for lang in official_language_list:
            lang = self.clean_val(lang)
            if lang:
                json_data["FEATURES"].append({"OFFICIAL_LANGUAGE": lang})

        # Process UN LO Code
        un_lo_code = self.clean_val(raw_data.get("un_locode", ""))
        if un_lo_code:
            json_data["FEATURES"].append({"UN_LO_CODE": un_lo_code})

        # Process IATA Code
        iata_code = self.clean_val(raw_data.get("iata_code", ""))
        if iata_code:
            json_data["FEATURES"].append({"IATA_CODE": iata_code})

        # Process International Calling Code
        intl_calling_code = self.clean_val(
            raw_data.get("international_calling_code", "")
        )
        if intl_calling_code:
            json_data["FEATURES"].append(
                {"INTERNATIONAL_CALLING_CODE": intl_calling_code}
            )

        # Process fax numbers
        fax_number_list = raw_data.get("fax_number", []) or []
        for fax in fax_number_list:
            fax = self.clean_val(fax)
            if fax:
                json_data["FEATURES"].append({"FAX_NUMBER": fax})

        for item in raw_data.get("pep_status", "").split("|"):
            if item.strip():
                json_data["FEATURES"].append({"STATUS_PEP": item.strip()})

        # Process PEP remarks
        pep_remarks_list = raw_data.get("pep_remarks", []) or []
        for remark in pep_remarks_list:
            remark = self.clean_val(remark)
            if remark:
                json_data["FEATURES"].append({"PEP_REMARKS": remark})

        # Process Sanction remarks
        sanction_remarks_list = raw_data.get("sanctions_remarks", []) or []
        for remark in sanction_remarks_list:
            remark = self.clean_val(remark)
            if remark:
                json_data["FEATURES"].append({"SANCTION_REMARKS": remark})

        # Process Watchlist remarks
        watchlist_remarks_list = raw_data.get("watchlists_remarks", []) or []
        for remark in watchlist_remarks_list:
            remark = self.clean_val(remark)
            if remark:
                json_data["FEATURES"].append({"WATCHLIST_REMARKS": remark})

        # Process Enforcement remarks
        enforcement_remarks_list = raw_data.get("enforcement_remarks", []) or []
        for remark in enforcement_remarks_list:
            remark = self.clean_val(remark)
            if remark:
                json_data["FEATURES"].append({"ENFORCEMENT_REMARKS": remark})

        # Process APC remarks
        apc_remarks_list = raw_data.get("apc_remarks", []) or []
        for remark in apc_remarks_list:
            remark = self.clean_val(remark)
            if remark:
                json_data["FEATURES"].append({"APC_REMARKS": remark})

        # Process sanctions status
        sanctions_status = self.clean_val(raw_data.get("sanctions_status", ""))
        if sanctions_status:
            json_data["FEATURES"].append({"STATUS_SANCTION": sanctions_status})

        # Process watchlist status
        watchlist_status = self.clean_val(raw_data.get("watchlist_status", ""))
        if watchlist_status:
            json_data["FEATURES"].append({"STATUS_WATCHLIST": watchlist_status})

        # Process sanctions status
        apc_status = self.clean_val(raw_data.get("apc_status", ""))
        if apc_status:
            json_data["FEATURES"].append({"STATUS_APC": apc_status})

        # Process sanctions status
        enforcement_status = self.clean_val(raw_data.get("enforcement_status", ""))
        if enforcement_status:
            json_data["FEATURES"].append({"STATUS_ENFORCEMENT": enforcement_status})

        # Process update category
        change_category = self.clean_val(raw_data.get("update_category", ""))
        if change_category:
            json_data["FEATURES"].append({"CHANGE_CATEGORY": change_category})

        # Append PEP, sanction, and watchlist statuses to json_data
        # Function to convert string representations to boolean
        def str_to_bool(value):
            if isinstance(value, str):
                value = value.lower()  # Normalize to lowercase
                if value in ("true", "t", "1"):
                    return "True"
                elif value in ("false", "f", "0"):
                    return "False"
            return (
                "True" if bool(value) else "False"
            )  # Convert to boolean and return as string

        # Extract and set statuses related to PEP, sanctions, and watchlists
        json_data["PEP_STATUS"] = str_to_bool(
            raw_data.get("is_pep", False)
        )  # Default to False if not found
        json_data["SANCTION_STATUS"] = str_to_bool(
            raw_data.get("is_sanction", False)
        )  # Default to False if not found
        json_data["WATCHLIST_STATUS"] = str_to_bool(
            raw_data.get("is_watchlist", False)
        )  # Default to False if not found
        json_data["ENFORCEMENT_STATUS"] = str_to_bool(
            raw_data.get("is_enforcement", False)
        )  # Default to False if not found
        json_data["APC_STATUS"] = str_to_bool(
            raw_data.get("is_apc", False)
        )  # Default to False if not found

        # Process sanction information as lists
        sanction_authority_list = raw_data.get("sanctions_authority", []) or []
        sanction_authority_country_list = (
            raw_data.get("sanctions_authority_country", []) or []
        )
        sanction_action_date_date_list = (
            raw_data.get("sanctions_action_date_date", []) or []
        )
        sanction_action_date_month_list = (
            raw_data.get("sanctions_action_date_month", []) or []
        )
        sanction_action_date_year_list = (
            raw_data.get("sanctions_action_date_year", []) or []
        )
        sanction_change_date_date_list = (
            raw_data.get("sanctions_change_date_date", []) or []
        )
        sanction_change_date_month_list = (
            raw_data.get("sanctions_change_date_month", []) or []
        )
        sanction_change_date_year_list = (
            raw_data.get("sanctions_change_date_year", []) or []
        )
        sanction_end_date_date_list = raw_data.get("sanctions_end_date_date", []) or []
        sanction_end_date_month_list = (
            raw_data.get("sanctions_end_date_month", []) or []
        )
        sanction_end_date_year_list = raw_data.get("sanctions_end_date_year", []) or []
        sanction_legal_action_type_list = (
            raw_data.get("sanctions_legal_action_type", []) or []
        )
        sanction_order_number_list = raw_data.get("sanctions_order_number", []) or []
        sanction_programme_name_list = (
            raw_data.get("sanctions_programme_name", []) or []
        )
        sanction_programme_country_list = (
            raw_data.get("sanctions_programme_country", []) or []
        )
        sanction_programme_country_code_list = (
            raw_data.get("sanctions_programme_country_code", []) or []
        )
        sanction_authority_ids_list = raw_data.get("sanction_authority_id", []) or []
        sanction_list_names_list = raw_data.get("sanctions_list_name", []) or []

        # Compile sanction data into features
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
            sanction_programme_country_code = self.clean_val(
                sanction_programme_country_code
            )
            sanction_authority_id = self.clean_val(sanction_authority_id)
            sanction_list_name = self.clean_val(sanction_list_name)

            json_data["FEATURES"].append(
                {
                    "SANCTION_AUTHORITY": sanction_authority,
                    "SANCTION_AUTHORITY_COUNTRY": sanction_authority_country,
                    "SANCTION_ACTION_DATE_DATE": sanction_action_date_date,
                    "SANCTION_ACTION_DATE_MONTH": sanction_action_date_month,
                    "SANCTION_ACTION_DATE_YEAR": sanction_action_date_year,
                    "SANCTION_CHANGE_DATE_DATE": sanction_change_date_date,
                    "SANCTION_CHANGE_DATE_MONTH": sanction_change_date_month,
                    "SANCTION_CHANGE_DATE_YEAR": sanction_change_date_year,
                    "SANCTION_END_DATE_DATE": sanction_end_date_date,
                    "SANCTION_END_DATE_MONTH": sanction_end_date_month,
                    "SANCTION_END_DATE_YEAR": sanction_end_date_year,
                    "SANCTION_LEGAL_ACTION_TYPE": sanction_legal_action_type,
                    "SANCTION_ORDER_NUMBER": sanction_order_number,
                    "SANCTION_PROGRAMME_NAME": sanction_programme_name,
                    "SANCTION_PROGRAMME_COUNTRY": sanction_programme_country,
                    "SANCTION_PROGRAMME_COUNTRY_CODE": sanction_programme_country_code,
                    "SANCTION_AUTHORITY_ID": sanction_authority_id,
                    "SANCTION_LIST_NAME": sanction_list_name,
                }
            )

        # Process associated individual and entity information as lists
        associated_individual_name_list = (
            raw_data.get("association_associated_individual_name", []) or []
        )
        associated_individual_position_list = (
            raw_data.get("association_associated_individual_position", []) or []
        )
        associated_entities_name_list = (
            raw_data.get("association_associated_entities_name", []) or []
        )

        # Compile associated data into features
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
            associated_individual_position = self.clean_val(
                associated_individual_position
            )
            associated_entities_name = self.clean_val(associated_entities_name)

            json_data["FEATURES"].append(
                {
                    "ASSOCIATED_INDIVIDUAL_NAME": associated_individual_name,
                    "ASSOCIATED_INDIVIDUAL_POSITION": associated_individual_position,
                    "ASSOCIATED_ENTITIES_NAME": associated_entities_name,
                }
            )

        # Extract and append restrictions to json_data['FEATURES']
        restrictions_list = raw_data.get("restrictions", []) or []
        for restrictions in restrictions_list:
            restrictions = self.clean_val(restrictions)
            if restrictions:
                json_data["FEATURES"].append({"RESTRICTIONS": restrictions})

        # Extract lists from raw_data related to watchlist information
        watchlist_authority_list = raw_data.get("watchlists_authority", []) or []
        watchlist_list_name_list = raw_data.get("watchlists_list_name", []) or []
        watchlist_list_abbreviation_list = (
            raw_data.get("watchlists_list_abbreviation", []) or []
        )
        watchlist_authority_country_list = (
            raw_data.get("watchlists_authority_country", []) or []
        )
        watchlist_action_date_date_list = (
            raw_data.get("watchlists_action_date_date", []) or []
        )
        watchlist_action_date_month_list = (
            raw_data.get("watchlists_action_date_month", []) or []
        )
        watchlist_action_date_year_list = (
            raw_data.get("watchlists_action_date_year", []) or []
        )
        watchlist_additional_information_list = (
            raw_data.get("watchlists_additional_information", []) or []
        )
        watchlist_list_id_list = raw_data.get("watchlists_list_id", []) or []

        # Loop through the watchlist-related lists simultaneously
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
            watchlist_additional_information = self.clean_val(
                watchlist_additional_information
            )
            watchlist_list_id = self.clean_val(watchlist_list_id)

            # Append each watchlist-related attribute to json_data['FEATURES']
            json_data["FEATURES"].append(
                {
                    "WATCHLIST_AUTHORITY": watchlist_authority,
                    "WATCHLIST_LIST_NAME": watchlist_list_name,
                    "WATCHLIST_LIST_ABBREVIATION": watchlist_list_abbreviation,
                    "WATCHLIST_AUTHORITY_COUNTRY": watchlist_authority_country,
                    "WATCHLIST_ACTION_DATE_DATE": watchlist_action_date_date,
                    "WATCHLIST_ACTION_DATE_MONTH": watchlist_action_date_month,
                    "WATCHLIST_ACTION_DATE_YEAR": watchlist_action_date_year,
                    "WATCHLIST_ADDITIONAL_INFORMATION": watchlist_additional_information,
                    "WATCHLIST_LIST_ID": watchlist_list_id,
                }
            )

        # Extract lists from raw_data related to Enforcement information
        enforcement_legal_action_type_list = (
            raw_data.get("enforcement_legal_action_type", []) or []
        )
        enforcement_legal_action_date_day_list = (
            raw_data.get("enforcement_legal_action_date_day", []) or []
        )
        enforcement_legal_action_date_month_list = (
            raw_data.get("enforcement_legal_action_date_month", []) or []
        )
        enforcement_legal_action_date_year_list = (
            raw_data.get("enforcement_legal_action_date_year", []) or []
        )
        enforcement_imprisonment_or_restriction_list = (
            raw_data.get("enforcement_imprisonment_or_restriction", []) or []
        )
        enforcement_fine_amount_in_local_currency_list = (
            raw_data.get("enforcement_fine_amount_in_local_currency", []) or []
        )
        enforcement_name_of_local_currency_list = (
            raw_data.get("enforcement_name_of_local_currency", []) or []
        )
        enforcement_fine_amount_in_usd_list = (
            raw_data.get("enforcement_fine_amount_in_usd", []) or []
        )
        enforcement_conversion_rate_list = (
            raw_data.get("enforcement_conversion_rate", []) or []
        )
        enforcement_primary_regulators_list = (
            raw_data.get("enforcement_primary_regulators", []) or []
        )
        enforcement_stated_regulations_list = (
            raw_data.get("enforcement_stated_regulations", []) or []
        )
        enforcement_enforcement_list_name_list = (
            raw_data.get("enforcement_enforcement_list_name", []) or []
        )
        enforcement_profile_summary_list = (
            raw_data.get("enforcement_profile_summary", []) or []
        )
        enforcement_reasoning_for_legal_actions_list = (
            raw_data.get("enforcement_reasoning_for_legal_actions", []) or []
        )
        enforcement_taxonomy_list = raw_data.get("enforcement_taxonomy", []) or []
        enforcement_event_id_list = raw_data.get("enforcement_event_id", []) or []

        # Loop through the Enforcement-related lists simultaneously
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
            enforcement_legal_action_type = self.clean_val(
                enforcement_legal_action_type
            )
            enforcement_legal_action_date_day = self.clean_val(
                enforcement_legal_action_date_day
            )
            enforcement_legal_action_date_month = self.clean_val(
                enforcement_legal_action_date_month
            )
            enforcement_legal_action_date_year = self.clean_val(
                enforcement_legal_action_date_year
            )
            enforcement_imprisonment_or_restriction = self.clean_val(
                enforcement_imprisonment_or_restriction
            )
            enforcement_fine_amount_in_local_currency = self.clean_val(
                enforcement_fine_amount_in_local_currency
            )
            enforcement_name_of_local_currency = self.clean_val(
                enforcement_name_of_local_currency
            )
            enforcement_fine_amount_in_usd = self.clean_val(
                enforcement_fine_amount_in_usd
            )
            enforcement_conversion_rate = self.clean_val(enforcement_conversion_rate)
            enforcement_primary_regulators = self.clean_val(
                enforcement_primary_regulators
            )
            enforcement_stated_regulations = self.clean_val(
                enforcement_stated_regulations
            )
            enforcement_enforcement_list_name = self.clean_val(
                enforcement_enforcement_list_name
            )
            enforcement_profile_summary = self.clean_val(enforcement_profile_summary)
            enforcement_reasoning_for_legal_actions = self.clean_val(
                enforcement_reasoning_for_legal_actions
            )
            enforcement_taxonomy = self.clean_val(enforcement_taxonomy)
            enforcement_event_id = self.clean_val(enforcement_event_id)

            # Append each Enforcement-related attribute to json_data['FEATURES']
            json_data["FEATURES"].append(
                {
                    "ENFORCEMENT_LEGAL_ACTION_TYPE": enforcement_legal_action_type,
                    "ENFORCEMENT_LEGAL_ACTION_DATE_DAY": enforcement_legal_action_date_day,
                    "ENFORCEMENT_LEGAL_ACTION_DATE_MONTH": enforcement_legal_action_date_month,
                    "ENFORCEMENT_LEGAL_ACTION_DATE_YEAR": enforcement_legal_action_date_year,
                    "ENFORCEMENT_IMPRISONMENT_OR_RESTRICTION": enforcement_imprisonment_or_restriction,
                    "ENFORCEMENT_FINE_AMOUNT_IN_LOCAL_CURRENCY": enforcement_fine_amount_in_local_currency,
                    "ENFORCEMENT_NAME_OF_LOCAL_CURRENCY": enforcement_name_of_local_currency,
                    "ENFORCEMENT_FINE_AMOUNT_IN_USD": enforcement_fine_amount_in_usd,
                    "ENFORCEMENT_CONVERSION_RATE": enforcement_conversion_rate,
                    "ENFORCEMENT_PRIMARY_REGULATORS": enforcement_primary_regulators,
                    "ENFORCEMENT_STATED_REGULATIONS": enforcement_stated_regulations,
                    "ENFORCEMENT_ENFORCEMENT_LIST_NAME": enforcement_enforcement_list_name,
                    "ENFORCEMENT_PROFILE_SUMMARY": enforcement_profile_summary,
                    "ENFORCEMENT_REASONING_FOR_LEGAL_ACTIONS": enforcement_reasoning_for_legal_actions,
                    "ENFORCEMENT_TAXONOMY": enforcement_taxonomy,
                    "ENFORCEMENT_EVENT_ID": enforcement_event_id,
                }
            )

        # Extract lists from raw_data related to APC information
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

        # Loop through the APC-related lists simultaneously
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
            json_data["FEATURES"].append(
                {
                    "APC_GROUP_ID": self.clean_val(apc_group_id),
                    "APC_ARTICLE_ID": self.clean_val(apc_article_id),
                    "APC_DATE_PUBLISHED": self.clean_val(apc_date_published),
                    "APC_MONTH_PUBLISHED": self.clean_val(apc_month_published),
                    "APC_YEAR_PUBLISHED": self.clean_val(apc_year_published),
                    "APC_HEADING": self.clean_val(apc_heading),
                    "APC_NEWS_LINK": self.clean_val(apc_news_link),
                    "APC_LANGUAGE": self.clean_val(apc_language),
                    "APC_NEWS_PROVIDER": self.clean_val(apc_news_provider),
                    "APC_SENTIMENT": self.clean_val(apc_sentiment),
                    "APC_SUMMARY": self.clean_val(apc_summary),
                    "APC_SOURCE_REPUTATION": self.clean_val(apc_source_reputation),
                    "APC_ARTICLE_TEXT": self.clean_val(apc_article_text),
                    "APC_SUMMARY_LEDE": self.clean_val(apc_summary_lede),
                    "APC_FRAMEWORKS_NAME": self.clean_val(apc_frameworks_name),
                    "APC_FRAMEWORKS_VERSION": self.clean_val(apc_frameworks_version),
                    "APC_RISK_SCORE": self.clean_val(apc_risk_score),
                    "APC_CATEGORIES": self.clean_val(apc_categories),
                    "APC_RISK_AREAS": self.clean_val(apc_risk_areas),
                    "APC_EVENTS": self.clean_val(apc_events),
                    "APC_KEYWORDS": self.clean_val(apc_keywords),
                    "APC_EVENT_STAGE": self.clean_val(apc_event_stage),
                    "APC_NER_TYPE": self.clean_val(apc_ner_type),
                    "APC_NER_ENTITIES": self.clean_val(apc_ner_entities),
                    "APC_NER_ATTRIBUTES": self.clean_val(apc_ner_attributes),
                    "APC_RELEVANCE_SCORE": self.clean_val(apc_relevance_score),
                    "APC_LOCATIONS": self.clean_val(apc_locations),
                    "APC_ARTICLE_CATEGORY": self.clean_val(apc_article_category),
                    "APC_NETWORK_MAP": self.clean_val(apc_network_map),
                    "APC_RISK_EVENT": self.clean_val(apc_risk_event),
                    "APC_EVENT_CHRONOLOGY": self.clean_val(apc_event_chronology),
                    "APC_REGULATORY_ACTION": self.clean_val(apc_regulatory_action),
                    "APC_REGULATOR": self.clean_val(apc_regulator),
                    "APC_PENALTY_AMOUNT": self.clean_val(apc_penalty_amount),
                }
            )

        # Extract court-related lists from raw_data safely using ast.literal_eval
        court_name_list = raw_data.get("litigation_court_name", []) or []
        number_of_cases_list = raw_data.get("litigation_number_of_cases", []) or []
        case_number_list = raw_data.get("litigation_case_number", []) or []
        litigation_date_date_list = raw_data.get("litigation_date_date", []) or []
        litigation_date_month_list = raw_data.get("litigation_date_month", []) or []
        litigation_date_year_list = raw_data.get("litigation_date_year", []) or []

        # Loop through court-related lists simultaneously
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
            json_data["FEATURES"].append(
                {
                    "LITIGATION_COURT_NAME": self.clean_val(court_name),
                    "LITIGATION_NUMBER_OF_CASES": self.clean_val(number_of_cases),
                    "LITIGATION_CASE_NUMBER": self.clean_val(case_number),
                    "LITIGATION_DATE_DATE": self.clean_val(litigation_date_date),
                    "LITIGATION_DATE_MONTH": self.clean_val(litigation_date_month),
                    "LITIGATION_DATE_YEAR": self.clean_val(litigation_date_year),
                }
            )

        # Extract lists from raw_data related to pincode information
        pincode_high_risk_area = self.clean_val(
            raw_data.get("pincode_high_risk_area", "")
        )
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
            json_data["FEATURES"].append(
                {
                    "PINCODE_HIGH_RISK_AREA": pincode_high_risk_area,
                    "PINCODE_RISK_TYPE": pincode_risk_type,
                    "PINCODE_CITY": pincode_city,
                    "PINCODE_DISTRICT": pincode_district,
                    "PINCODE_STATE": pincode_state,
                    "PINCODE_COUNTRY": pincode_country,
                }
            )

        # Process others information as lists
        others_authority_list = raw_data.get("others_authority", []) or []
        others_list_name_list = raw_data.get("others_list_name", []) or []
        others_order_list = raw_data.get("others_order", []) or []
        others_programme_list = raw_data.get("others_programme", []) or []
        others_event_start_date_date_list = (
            raw_data.get("others_event_start_date_date", []) or []
        )
        others_event_start_date_month_list = (
            raw_data.get("others_event_start_date_month", []) or []
        )
        others_event_start_date_year_list = (
            raw_data.get("others_event_start_date_year", []) or []
        )
        others_event_end_date_date_list = (
            raw_data.get("others_event_end_date_date", []) or []
        )
        others_event_end_date_month_list = (
            raw_data.get("others_event_end_date_month", []) or []
        )
        others_event_end_date_year_list = (
            raw_data.get("others_event_end_date_year", []) or []
        )
        others_associated_subject_type_list = (
            raw_data.get("others_associated_subject_type", []) or []
        )
        others_event_summary_list = raw_data.get("others_event_summary", []) or []
        others_reasoning_taxonomy_list = (
            raw_data.get("others_reasoning_taxonomy", []) or []
        )

        # Loop through the lists simultaneously and clean values
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
            json_data["FEATURES"].append(
                {
                    "OTHERS_AUTHORITY": self.clean_val(others_authority),
                    "OTHERS_LIST_NAME": self.clean_val(others_list_name),
                    "OTHERS_ORDER": self.clean_val(others_order),
                    "OTHERS_PROGRAMME": self.clean_val(others_programme),
                    "OTHERS_EVENT_START_DATE_DATE": self.clean_val(
                        others_event_start_date_date
                    ),
                    "OTHERS_EVENT_START_DATE_MONTH": self.clean_val(
                        others_event_start_date_month
                    ),
                    "OTHERS_EVENT_START_DATE_YEAR": self.clean_val(
                        others_event_start_date_year
                    ),
                    "OTHERS_EVENT_END_DATE_DATE": self.clean_val(
                        others_event_end_date_date
                    ),
                    "OTHERS_EVENT_END_DATE_MONTH": self.clean_val(
                        others_event_end_date_month
                    ),
                    "OTHERS_EVENT_END_DATE_YEAR": self.clean_val(
                        others_event_end_date_year
                    ),
                    "OTHERS_ASSOCIATED_SUBJECT_TYPE": self.clean_val(
                        others_associated_subject_type
                    ),
                    "OTHERS_EVENT_SUMMARY": self.clean_val(others_event_summary),
                    "OTHERS_REASONING_TAXONOMY": self.clean_val(
                        others_reasoning_taxonomy
                    ),
                }
            )

        # Remove empty dictionaries or dictionaries with only empty values
        json_data["FEATURES"] = [
            item
            for item in json_data["FEATURES"]
            if any(value for value in item.values() if value not in [None, "", [], {}])
        ]

        # --remove empty attributes and capture the stats
        json_data = self.remove_empty_tags(json_data)
        self.capture_mapped_stats(json_data)

        return json_data

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
                    return ""  # empty list → blank
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
    parser.add_argument(
        "-i", "--input_file", dest="input_file", help="the name of the input file"
    )
    parser.add_argument(
        "-o", "--output_file", dest="output_file", help="the name of the output file"
    )
    parser.add_argument(
        "-l",
        "--log_file",
        dest="log_file",
        help="optional name of the statistics log file",
    )
    parser.add_argument(
        "-d", "--data_source", dest="data_source", help="data source code (required)"
    )
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
    run_status = (
        "completed in" if not shut_down else "aborted after"
    ) + f" {elapsed_mins} minutes"
    print(
        f"{input_row_count} rows processed, {output_row_count} rows written, {run_status}\n"
    )

    output_file_handle.close()
    input_file_handle.close()

    sys.exit(0)