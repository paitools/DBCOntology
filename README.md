# CANDI - A Semantic Framework for CAN bus Data Modeling and System Integration
![Contributions](https://img.shields.io/badge/Format-RDF/XML-blue)
![Contributions](https://img.shields.io/badge/Format-TTL-blue)
![Contributions](https://img.shields.io/badge/Language-Python-blue)

CANDI is a semantic framework for dynamic CAN bus data decoding, system integration, and E2E deployment automation. CANDI combines virtual knowledge graphs (VKG) with OBDA principles to bridge raw data streams with structured semantic representations, enabling runtime message decoding, semantically governed diagnostics, and real-time analytics.

## DBC Ontology:

DBC is the ontology for CAN bus communication systems that extends the W3C SSN/SOSA standards and formalizes the semantics of signals, messages, electronic control units (ECUs), and data-logging processes. Grounded in the CAN database (DBC-file) protocol, it provides an interoperable and machine-interpretable schema for transportation and embedded-systems data. The DBC lies at the core of CANDI’s semantic reasoning and data integration.

## Ontology Documentation:

Automatically generated Ontology Specification.

[![Documentation](https://img.shields.io/badge/Documentation-Ontology_Specification-blue)](https://paitools.github.io/DBCOntology/documentation/index-en.html)

## CANDI User Guide

Running **CANDI** on user hardware involves two automated steps:

1. **Create the Knowledge Graph Matrix (KGM)**
2. **Deploy the Framework**


### 1. KGM Creation

1. Set the DBC file path in the `load_dbc.py` configuration (e.g., `DBC/boening.dbc`).
2. Run the script:
   ```bash
   python3 load_dbc.py


## License

All resources are licensed under the [Creative Commons Attribution-NonCommercial-ShareAlike 4.0 International](https://creativecommons.org/licenses/by-nc-sa/4.0/) license.

![License: CC BY-NC-SA 4.0](https://img.shields.io/badge/License-CC%20BY--NC--SA%204.0-lightgrey.svg)

