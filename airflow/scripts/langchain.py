

import os
import json
import logging
from typing import List, Dict, Any
from datetime import datetime


from langchain_anthropic import ChatAnthropic
from langchain_core.prompts import ChatPromptTemplate
from langchain_core.output_parsers import JsonOutputParser
from pydantic import BaseModel, Field

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)




class SentimentAnalysis(BaseModel):

    sentiment_score: float = Field(description="Sentiment score from -1 (very negative) to 1 (very positive)")
    sentiment_label: str = Field(description="One of: positive, negative, neutral")
    confidence: float = Field(description="Confidence score from 0 to 1")
    emotions: List[str] = Field(description="List of detected emotions (joy, anger, fear, sadness, etc.)")
    key_phrases: List[str] = Field(description="Important phrases that influenced the sentiment")
    reasoning: str = Field(description="Brief explanation of the sentiment analysis")


class BrandMention(BaseModel):

    brand_name: str = Field(description="Name of the brand mentioned")
    context: str = Field(description="Context in which the brand was mentioned")
    sentiment_towards_brand: str = Field(description="Sentiment specifically towards this brand")
    comparison_with: List[str] = Field(description="Other brands mentioned in comparison", default=[])
    purchase_intent: str = Field(description="Evidence of purchase intent: high, medium, low, none")


class SocialMediaInsights(BaseModel):

    sentiment: SentimentAnalysis
    brands: List[BrandMention]
    topics: List[str] = Field(description="Main topics discussed")
    user_intent: str = Field(description="User's primary intent: complaint, praise, inquiry, comparison, general")
    urgency: str = Field(description="Urgency level: high, medium, low")
    requires_response: bool = Field(description="Whether this post requires a brand response")
    response_priority: str = Field(description="Priority for response: high, medium, low")




class LangChainSentimentAnalyzer:


    def __init__(self, api_key: str = None, model: str = "claude-sonnet-4-20250514"):

        self.api_key = api_key or os.getenv("ANTHROPIC_API_KEY")
        if not self.api_key:
            logger.warning("No Anthropic API key provided. LangChain features will be limited.")
            self.llm = None
        else:
            self.llm = ChatAnthropic(
                model=model,
                temperature=0,
                anthropic_api_key=self.api_key
            )
            logger.info(f"✓ Initialized LangChain with model: {model}")

    def analyze_sentiment(self, text: str) -> Dict[str, Any]:

        if not self.llm:
            return self._fallback_sentiment(text)

        try:

            parser = JsonOutputParser(pydantic_object=SentimentAnalysis)


            prompt = ChatPromptTemplate.from_messages([
                ("system", """You are an expert sentiment analyst for social media posts.
                Analyze the sentiment with nuance, considering context, sarcasm, and emotional tone.

                {format_instructions}"""),
                ("user", "Analyze the sentiment of this social media post:\n\n{text}")
            ])


            chain = prompt | self.llm | parser


            result = chain.invoke({
                "text": text,
                "format_instructions": parser.get_format_instructions()
            })

            logger.info(f"✓ Sentiment analyzed: {result['sentiment_label']} ({result['sentiment_score']:.2f})")
            return result

        except Exception as e:
            logger.error(f"Error in sentiment analysis: {e}")
            return self._fallback_sentiment(text)

    def extract_brand_mentions(self, text: str, known_brands: List[str] = None) -> List[Dict[str, Any]]:

        if not self.llm:
            return self._fallback_brands(text, known_brands)

        try:
            prompt = ChatPromptTemplate.from_messages([
                ("system", """You are an expert at identifying brand mentions in social media posts.
                Extract all brand names mentioned, the context, and sentiment towards each brand.

                {format_instructions}

                Known brands to look for: {brands}"""),
                ("user", "{text}")
            ])

            parser = JsonOutputParser(pydantic_object=BrandMention)
            chain = prompt | self.llm | parser

            result = chain.invoke({
                "text": text,
                "brands": ", ".join(known_brands) if known_brands else "any brands",
                "format_instructions": parser.get_format_instructions()
            })


            if isinstance(result, dict):
                result = [result]

            logger.info(f"✓ Extracted {len(result)} brand mentions")
            return result

        except Exception as e:
            logger.error(f"Error in brand extraction: {e}")
            return self._fallback_brands(text, known_brands)

    def get_comprehensive_insights(self, text: str, brands: List[str] = None) -> Dict[str, Any]:

        if not self.llm:
            return self._fallback_insights(text)

        try:
            prompt = ChatPromptTemplate.from_messages([
                ("system", """You are an expert social media analyst.
                Provide comprehensive insights about this post including:
                - Detailed sentiment analysis
                - Brand mentions with context
                - Main topics and themes
                - User intent and urgency
                - Whether a brand response is needed

                {format_instructions}

                Known brands: {brands}"""),
                ("user", "{text}")
            ])

            parser = JsonOutputParser(pydantic_object=SocialMediaInsights)
            chain = prompt | self.llm | parser

            result = chain.invoke({
                "text": text,
                "brands": ", ".join(brands) if brands else "any brands",
                "format_instructions": parser.get_format_instructions()
            })

            logger.info(f"✓ Generated comprehensive insights")
            return result

        except Exception as e:
            logger.error(f"Error generating insights: {e}")
            return self._fallback_insights(text)

    def batch_analyze(self, texts: List[str], brands: List[str] = None) -> List[Dict[str, Any]]:

        results = []

        logger.info(f"Starting batch analysis of {len(texts)} posts...")

        for i, text in enumerate(texts):
            try:
                result = self.get_comprehensive_insights(text, brands)
                result['original_text'] = text
                result['processed_at'] = datetime.now().isoformat()
                results.append(result)

                if (i + 1) % 10 == 0:
                    logger.info(f"Progress: {i + 1}/{len(texts)} posts analyzed")

            except Exception as e:
                logger.error(f"Error analyzing post {i + 1}: {e}")
                results.append({
                    'original_text': text,
                    'error': str(e),
                    'processed_at': datetime.now().isoformat()
                })

        logger.info(f"✓ Batch analysis completed: {len(results)} posts")
        return results

    def generate_summary_report(self, insights: List[Dict[str, Any]]) -> str:

        if not self.llm:
            return "LLM not available for report generation"

        try:

            summary_data = {
                'total_posts': len(insights),
                'positive': sum(1 for i in insights if i.get('sentiment', {}).get('sentiment_label') == 'positive'),
                'negative': sum(1 for i in insights if i.get('sentiment', {}).get('sentiment_label') == 'negative'),
                'neutral': sum(1 for i in insights if i.get('sentiment', {}).get('sentiment_label') == 'neutral'),
                'high_priority': sum(1 for i in insights if i.get('response_priority') == 'high'),
                'brands': list(set([b['brand_name'] for i in insights for b in i.get('brands', [])])),
                'common_topics': self._get_common_topics(insights)
            }

            prompt = ChatPromptTemplate.from_messages([
                ("system", """You are an expert social media analyst creating executive reports.
                Create a concise, actionable summary report based on the analyzed social media data.
                Focus on key insights, trends, and recommendations."""),
                ("user", """Generate a comprehensive summary report based on this data:

                Total Posts Analyzed: {total_posts}
                Sentiment Breakdown:
                - Positive: {positive}
                - Negative: {negative}
                - Neutral: {neutral}

                High Priority Posts: {high_priority}
                Brands Mentioned: {brands}
                Common Topics: {topics}

                Provide:
                1. Overall sentiment trend
                2. Key insights
                3. Urgent issues requiring attention
                4. Recommendations for brand response
                """)
            ])

            chain = prompt | self.llm

            report = chain.invoke({
                'total_posts': summary_data['total_posts'],
                'positive': summary_data['positive'],
                'negative': summary_data['negative'],
                'neutral': summary_data['neutral'],
                'high_priority': summary_data['high_priority'],
                'brands': ', '.join(summary_data['brands'][:10]),
                'topics': ', '.join(summary_data['common_topics'][:10])
            })

            logger.info(" Summary report generated")
            return report.content

        except Exception as e:
            logger.error(f"Error generating summary report: {e}")
            return f"Error generating report: {e}"



    def _fallback_sentiment(self, text: str) -> Dict[str, Any]:

        text_lower = text.lower()

        positive_words = ['good', 'great', 'love', 'excellent', 'amazing', 'fantastic']
        negative_words = ['bad', 'hate', 'terrible', 'awful', 'poor', 'worst']

        pos_count = sum(word in text_lower for word in positive_words)
        neg_count = sum(word in text_lower for word in negative_words)

        total = pos_count + neg_count
        if total == 0:
            score = 0.0
            label = 'neutral'
        else:
            score = (pos_count - neg_count) / total
            if score > 0.2:
                label = 'positive'
            elif score < -0.2:
                label = 'negative'
            else:
                label = 'neutral'

        return {
            'sentiment_score': score,
            'sentiment_label': label,
            'confidence': 0.5,
            'emotions': [],
            'key_phrases': [],
            'reasoning': 'Fallback rule-based analysis'
        }

    def _fallback_brands(self, text: str, known_brands: List[str] = None) -> List[Dict[str, Any]]:

        if not known_brands:
            known_brands = ['Apple', 'Google', 'Microsoft', 'Amazon', 'Tesla']

        found_brands = []
        for brand in known_brands:
            if brand.lower() in text.lower():
                found_brands.append({
                    'brand_name': brand,
                    'context': text,
                    'sentiment_towards_brand': 'unknown',
                    'comparison_with': [],
                    'purchase_intent': 'unknown'
                })

        return found_brands

    def _fallback_insights(self, text: str) -> Dict[str, Any]:

        sentiment = self._fallback_sentiment(text)
        brands = self._fallback_brands(text)

        return {
            'sentiment': sentiment,
            'brands': brands,
            'topics': [],
            'user_intent': 'unknown',
            'urgency': 'low',
            'requires_response': False,
            'response_priority': 'low'
        }

    def _get_common_topics(self, insights: List[Dict[str, Any]]) -> List[str]:

        topics = []
        for insight in insights:
            topics.extend(insight.get('topics', []))


        from collections import Counter
        topic_counts = Counter(topics)


        return [topic for topic, count in topic_counts.most_common(10)]




def example_usage():

    analyzer = LangChainSentimentAnalyzer()


    posts = [
        "Just got my new Apple iPhone 15 and I'm absolutely loving it! Best phone I've ever had!",
        "Google's new AI features are terrible. Very disappointed with this update.",
        "Comparing Microsoft and Apple for enterprise solutions. Need recommendations.",
        "Amazon delivery was late AGAIN. This is the third time this month!",
        "Tesla's new autopilot feature is amazing! Revolutionary technology."
    ]

    brands = ['Apple', 'Google', 'Microsoft', 'Amazon', 'Tesla']


    print("\n" + "=" * 60)
    print("Single Post Analysis")
    print("=" * 60)
    result = analyzer.analyze_sentiment(posts[0])
    print(json.dumps(result, indent=2))


    print("\n" + "=" * 60)
    print("Brand Mention Extraction")
    print("=" * 60)
    brand_mentions = analyzer.extract_brand_mentions(posts[0], brands)
    print(json.dumps(brand_mentions, indent=2))


    print("\n" + "=" * 60)
    print("Comprehensive Insights")
    print("=" * 60)
    insights = analyzer.get_comprehensive_insights(posts[0], brands)
    print(json.dumps(insights, indent=2))


    print("\n" + "=" * 60)
    print("Batch Analysis")
    print("=" * 60)
    batch_results = analyzer.batch_analyze(posts, brands)


    print("\n" + "=" * 60)
    print("Summary Report")
    print("=" * 60)
    report = analyzer.generate_summary_report(batch_results)
    print(report)


if __name__ == "__main__":
    example_usage()