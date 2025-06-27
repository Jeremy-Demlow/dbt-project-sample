# Synthetic Data Generator for Snowflake Cortex LLM Demo

This Python script generates realistic synthetic data for the Snowflake + dbt + Cortex LLM Functions Quickstart. It creates four JSON files with correlated data:

1. `customers.json` - Base customer information and personas
2. `customer_interactions.json` - Customer interaction notes and metadata
3. `product_reviews.json` - Product reviews with ratings in multiple languages
4. `support_tickets.json` - Customer support tickets with detailed descriptions

## Features

- Generates configurable number of records
- Creates realistic text using a lightweight open-source LLM (TinyLlama)
- Maintains consistency with customer personas across datasets
- Includes built-in correlations and signal patterns
- Fallback text generation if LLM isn't available
- Multilingual support for product reviews
- Parallel processing for faster data generation
- Configurable output directory

## Requirements

- Python 3.7+
- PyTorch
- Transformers library
- Snowflake Cortex LLM Functions

## Installation

```bash
# Create a virtual environment (recommended)
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install dependencies
pip install torch transformers
```

## Usage

Basic usage with default settings:

```bash
python generate_synthetic_data.py
```

Specify number of records:

```bash
python generate_synthetic_data.py --num-records 100
```

Change output directory:

```bash
python generate_synthetic_data.py --output-dir /path/to/data
```

Enable parallel processing:

```bash
python generate_synthetic_data.py --parallel
```

Select a specific LLM model:

```bash
python generate_synthetic_data.py --model distilgpt2
```

Use templates only (no LLM):

```bash
python generate_synthetic_data.py --use-templates-only
```

Adjust template usage ratio:

```bash
python generate_synthetic_data.py --template-ratio 0.7
```

### Available LLM Models

The script supports several lightweight open-source models for text generation:

- **distilgpt2** (82M parameters)
  - Small, fast distilled version of GPT-2
  - Good balance of quality and speed
  - Default model
  - Limited translation capabilities
  - Best for English text generation

- **microsoft/MiniLM-L12-H384-uncased** (33M parameters)
  - Very small and fast model by Microsoft
  - Optimized for speed
  - Good for basic text generation
  - No direct translation support
  - English-only

- **distilroberta-base** (82M parameters)
  - Lightweight RoBERTa model
  - Good for structured text
  - Balanced performance
  - Limited translation capabilities
  - Best for English text generation

- **facebook/opt-125m** (125M parameters)
  - Tiny OPT model from Meta
  - Good for conversational text
  - Fast inference
  - Limited translation capabilities
  - Best for English text generation

### Usage Examples

#### Using distilgpt2 Without Templates

To generate data using only the distilgpt2 model without any templates:

```bash
python generate_synthetic_data.py --model distilgpt2 --template-ratio 0.0 --num-records 5
```

This command will:
- Use distilgpt2 for all text generation
- Skip template-based generation completely
- Generate more varied but potentially less consistent text
- Take longer to run than template-based generation
- Produce English-only content
- Generate only 5 records (instead of default 100)

Example output:
```
Loading model: distilgpt2
Generating 5 records...
Generating customer base...
Generating product reviews...
Generating support tickets...
Generation complete! Output files:
- customers.json
- product_reviews.json
- support_tickets.json
```

Note: Using no templates will result in:
- More natural language variation
- Longer generation time
- Less predictable output
- English-only content
- No guaranteed consistency in sentiment patterns

### Translation Support

The script handles translations through a hybrid approach:

1. **Template-based Translation**
   - Pre-defined templates in multiple languages
   - Supports: English, Spanish, French, German, Italian, Portuguese
   - High-quality, consistent translations
   - Used for product reviews and support tickets

2. **Phrase-based Translation**
   - Key phrases translated using predefined dictionaries
   - Maintains consistency across datasets
   - Used for common expressions and sentiment indicators

3. **Fallback Mechanism**
   - If LLM translation is not available, uses template-based translation
   - Ensures all non-English content is properly generated
   - Maintains data quality and consistency

### Model Selection Guidelines

- For fastest generation: Use `microsoft/MiniLM-L12-H384-uncased`
- For best quality: Use `distilgpt2` (default)
- For structured text: Use `distilroberta-base`
- For conversational text: Use `facebook/opt-125m`
- For multilingual content: Use any model with template-based translation

Note: For best results with multilingual content, use the template-based translation system rather than relying on model translation capabilities.

### Template Usage

The script uses a hybrid approach combining LLM generation with templates:

- Templates provide consistent, high-quality base content
- LLM adds variety and natural language
- Template ratio controls the balance (0.0-1.0)
  - Higher values = more templates = faster generation
  - Lower values = more LLM = more variety but slower

## Generated Data Characteristics

### Customer Personas

The script creates consistent customer personas that influence all three datasets:

- **Satisfied customers**: High ratings, positive interactions, few support tickets
- **Frustrated customers**: Low ratings, negative interactions, many support tickets
- **Neutral customers**: Average ratings, mixed interactions, moderate support tickets
- **Mixed customers**: Variable ratings and behavior
- **New customers**: Recently joined, limited history
- **Improving customers**: Showing positive trends in satisfaction
- **Complex cases**: Multiple interaction patterns and varied sentiment

### Signal/Correlations

- Customers with negative reviews tend to have more support tickets
- Frustrated customers have more recent interactions
- Support ticket categories correlate with customer sentiment
- Review ratings follow persona expectations
- Language distribution in reviews follows realistic patterns
- Sentiment trends align with customer behavior patterns
- Ticket counts correlate with frustration levels

### Data Fields

#### Customer Base
- Customer ID
- Persona type
- Sign-up date
- Products owned
- Lifetime value
- Customer segment
- Account status
- Last interaction date
- Total interactions
- Average sentiment

#### Customer Interactions
- Customer ID
- Interaction date
- Interaction type
- Sentiment score
- Interaction notes
- Metadata (channel, duration, etc.)

#### Product Reviews
- Customer ID
- Product ID
- Rating (1-5)
- Review text
- Language
- Review date
- Helpful votes
- Review sentiment

#### Support Tickets
- Ticket ID
- Customer ID
- Ticket date
- Category
- Priority
- Status
- Description
- Resolution notes
- Sentiment score

### Fallback Mechanism

If the TinyLlama model cannot be loaded (due to memory constraints or missing dependencies), the script falls back to template-based text generation, ensuring it always produces usable output. The fallback system:

- Uses predefined templates for different types of content
- Maintains consistency with customer personas
- Generates realistic variations in text
- Supports multiple languages
- Preserves data relationships

## Integration with Quickstart

The generated JSON files can be used directly with the Snowflake + dbt + Cortex LLM Functions Quickstart. After generating the data:

1. Upload the files to a publicly accessible S3 bucket
2. Update the stage URL in the Snowflake setup script
3. Follow the remaining quickstart instructions

## Customization

You can modify the script to:

- Add more customer personas
- Adjust the correlation patterns
- Change the text generation prompts
- Add additional fields to the datasets
- Modify the language distribution
- Adjust sentiment patterns
- Change the review rating distribution
- Customize ticket categories and priorities

## Performance Optimization

The script includes several performance features:

- Parallel processing for faster generation
- Batch processing of items
- Memory-efficient text generation
- Configurable batch sizes
- Progress tracking
- Error handling and recovery

## License

This script is provided under the MIT license.
