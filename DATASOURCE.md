# PubMed Central Open Access Subset Documentation

This document provides an overview of the PubMed Central (PMC) Open Access (OA) Subset, which is the primary data source for the `py-load-pubmedcentral` ETL tool.

## Introduction

The PMC Open Access Subset is a part of the larger PubMed Central digital archive of biomedical and life sciences journal literature. It contains millions of articles that have been made available under licenses that permit reuse, such as Creative Commons licenses. This makes the subset a valuable resource for text mining, research, and other applications that require bulk access to scientific literature.

It is important to note that not all articles in PubMed Central are part of the Open Access Subset. Many articles in PMC are protected by copyright and have more restrictive usage terms. This ETL tool is designed to work specifically with the more permissive Open Access Subset.

## Data Access

The PMC Open Access Subset can be accessed in two main ways that are supported by this ETL tool:

1.  **FTP Service:** The National Center for Biotechnology Information (NCBI) provides an FTP server where the entire Open Access Subset, as well as incremental updates, can be downloaded. This is a common method for obtaining the data in bulk.

2.  **Amazon S3 (Cloud Service):** The dataset is also available in an Amazon S3 bucket as part of the AWS Open Data Sponsorship Program. This is often a faster and more convenient way to access the data, especially if you are running this ETL tool within the AWS ecosystem.

This tool's `pmc-sync` command allows you to choose between these two sources (`ftp` or `s3`) when performing a `full-load`.

## Data Format

The articles in the Open Access Subset are provided in a few different formats, but this ETL tool specifically uses the **XML format**. Each article is described by an XML file that contains:

*   **Metadata:** Information about the article, such as the title, authors, journal, publication date, and more.
*   **Full Text:** The complete text of the article, typically structured into sections like abstract, introduction, methods, results, and conclusion.
*   **References:** A list of citations made by the article.

The XML files adhere to the Journal Article Tag Suite (JATS) standard, which is a widely used XML format for scientific literature. This ETL tool is built to parse this specific XML structure and load its contents into the database.

## Terms of Use

While the articles in the Open Access Subset are licensed for reuse, the specific terms can vary from article to article. The licenses are generally categorized as follows:

*   **Commercial Use Allowed:** Includes licenses like CC0, CC BY, CC BY-SA, and CC BY-ND.
*   **Non-Commercial Use Only:** Includes licenses like CC BY-NC, CC BY-NC-SA, and CC BY-NC-ND.
*   **Other:** May include custom licenses or no machine-readable license.

**It is the sole responsibility of the user of this ETL tool to be aware of and comply with the license terms for each article.** The license information is typically included within the article's XML file. Failure to adhere to the license terms can constitute copyright infringement.

## Important Considerations

*   **Authorized Use Only:** Systematic or bulk downloading of content from the main PubMed Central website is strictly prohibited. You must use the designated bulk access methods (FTP, S3, OAI, etc.) for which this tool is designed.
*   **Data Updates:** The Open Access Subset is updated regularly. This tool's `delta-load` command is designed to fetch these incremental updates to keep your local database synchronized.
*   **Data Completeness:** While the subset is large, it does not contain all articles from PMC. Its contents are determined by the licensing agreements with publishers.
