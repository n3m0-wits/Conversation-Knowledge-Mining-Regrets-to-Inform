import argparse

from azure.identity import AzureCliCredential
from azure.search.documents.indexes import SearchIndexClient
from azure.search.documents.indexes.models import (
    AzureOpenAIVectorizer,
    AzureOpenAIVectorizerParameters,
    HnswAlgorithmConfiguration,
    SearchField,
    SearchFieldDataType,
    SearchIndex,
    SemanticConfiguration,
    SemanticField,
    SemanticPrioritizedFields,
    SemanticSearch,
    SimpleField,
    VectorSearch,
    VectorSearchProfile,
)

# Get parameters from command line
p = argparse.ArgumentParser()
p.add_argument("--search_endpoint", required=True)
p.add_argument("--openai_endpoint", required=True)
p.add_argument("--embedding_model", required=True)
args = p.parse_args()

SEARCH_ENDPOINT = args.search_endpoint
OPENAI_ENDPOINT = args.openai_endpoint
EMBEDDING_MODEL = args.embedding_model

INDEX_NAME = "call_transcripts_index"


def create_search_index():
    """Create or update search index for email records and embeddings."""
    credential = AzureCliCredential(process_timeout=30)
    index_client = SearchIndexClient(endpoint=SEARCH_ENDPOINT, credential=credential)

    fields = [
        SimpleField(name="id", type=SearchFieldDataType.String, key=True),
        SimpleField(name="chunk_id", type=SearchFieldDataType.String, filterable=True),
        SearchField(name="content", type=SearchFieldDataType.String, searchable=True),
        SearchField(name="sourceurl", type=SearchFieldDataType.String, searchable=True),
        SimpleField(
            name="source_message_key",
            type=SearchFieldDataType.String,
            filterable=True,
            sortable=True,
        ),
        SearchField(name="company", type=SearchFieldDataType.String, filterable=True, searchable=True),
        SearchField(name="portal", type=SearchFieldDataType.String, filterable=True, searchable=True),
        SearchField(name="category", type=SearchFieldDataType.String, filterable=True, searchable=True),
        SearchField(name="urgency", type=SearchFieldDataType.String, filterable=True),
        SimpleField(name="action_required", type=SearchFieldDataType.Boolean, filterable=True),
        SimpleField(name="sent_datetime", type=SearchFieldDataType.DateTimeOffset, filterable=True, sortable=True),
        SearchField(
            name="contentVector",
            type=SearchFieldDataType.Collection(SearchFieldDataType.Single),
            vector_search_dimensions=1536,
            vector_search_profile_name="myHnswProfile",
        ),
    ]

    vector_search = VectorSearch(
        algorithms=[HnswAlgorithmConfiguration(name="myHnsw")],
        profiles=[
            VectorSearchProfile(
                name="myHnswProfile",
                algorithm_configuration_name="myHnsw",
                vectorizer_name="myOpenAI",
            )
        ],
        vectorizers=[
            AzureOpenAIVectorizer(
                vectorizer_name="myOpenAI",
                kind="azureOpenAI",
                parameters=AzureOpenAIVectorizerParameters(
                    resource_url=OPENAI_ENDPOINT,
                    deployment_name=EMBEDDING_MODEL,
                    model_name=EMBEDDING_MODEL,
                ),
            )
        ],
    )

    semantic_config = SemanticConfiguration(
        name="my-semantic-config",
        prioritized_fields=SemanticPrioritizedFields(
            keywords_fields=[
                SemanticField(field_name="company"),
                SemanticField(field_name="category"),
            ],
            content_fields=[SemanticField(field_name="content")],
        ),
    )

    index = SearchIndex(
        name=INDEX_NAME,
        fields=fields,
        vector_search=vector_search,
        semantic_search=SemanticSearch(configurations=[semantic_config]),
    )

    result = index_client.create_or_update_index(index)
    print(f"✓ Search index '{result.name}' created")


create_search_index()
