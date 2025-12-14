

import os
import sys
from datetime import datetime, timedelta
import json
import logging


sys.path.append('/opt/airflow/scripts')

from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.bash import BashOperator
from airflow.models.dag import DAG


try:
    from langchain_sentiment import LangChainSentimentAnalyzer
    LANGCHAIN_AVAILABLE = True
except ImportError:
    LANGCHAIN_AVAILABLE = False
    LangChainSentimentAnalyzer = None

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


default_args = {
    'owner': 'data-team',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}


dag = DAG(
    'sentiment_pipeline_with_langchain',
    default_args=default_args,
    description='Social media sentiment analysis with LangChain AI insights',
    schedule_interval='@daily',
    catchup=False,
    max_active_runs=1,
    tags=['sentiment', 'social-media', 'langchain', 'ai']
)


def check_langchain_available(**context):

    try:
        import langchain_anthropic
        api_key = os.getenv('ANTHROPIC_API_KEY')

        if api_key:
            logger.info("LangChain available with API key")
            context['task_instance'].xcom_push(key='use_langchain', value=True)
            return 'langchain_sentiment_analysis'
        else:
            logger.warning(" LangChain available but no API key found")
            context['task_instance'].xcom_push(key='use_langchain', value=False)
            return 'basic_sentiment_analysis'

    except ImportError:
        logger.warning(" LangChain not installed, using basic analysis")
        context['task_instance'].xcom_push(key='use_langchain', value=False)
        return 'basic_sentiment_analysis'


def langchain_sentiment_analysis(**context):

    if not LANGCHAIN_AVAILABLE:
        logger.error("LangChain not available!")
        raise ImportError("LangChain module not found")

    logger.info("Starting LangChain sentiment analysis...")

    try:

        analyzer = LangChainSentimentAnalyzer()

        posts = [
            {
                'id': '1',
                'text': "Just got my new Apple iPhone 15 Pro! The camera is absolutely incredible. Best upgrade I've made in years!",
                'brand': 'Apple',
                'platform': 'twitter'
            },

            {
                'id': '2',
                'text': "Google's customer service is the worst. Been waiting 3 hours for support. Never buying Pixel again.",
                'brand': 'Google',
                'platform': 'reddit'
            },

            {
                'id': '3',
                'text': "Thinking about switching from Microsoft to Apple for my work laptop. Anyone have experience with both?",
                'brand': 'Microsoft',
                'platform': 'twitter'
            },

            {
                'id': '4',
                'text': "Amazon Prime delivery is amazing! Ordered yesterday, arrived today. Couldn't be happier.",
                'brand': 'Amazon',
                'platform': 'instagram'
            },

            {
                'id': '5',
                'text': "Tesla's autopilot almost caused an accident today. This is dangerous and needs to be fixed immediately!",
                'brand': 'Tesla',
                'platform': 'facebook'
            }
        ]
        brands = ['Apple', 'Google', 'Microsoft', 'Amazon', 'Tesla']

        texts = [post['text'] for post in posts]

        insights = analyzer.batch_analyze(texts, brands)

        for i, insight in enumerate(insights):
            insight['post_id'] = posts[i]['id']
            insight['platform'] = posts[i]['platform']
            insight['analyzed_at'] = datetime.now().isoformat()


        summary_report = analyzer.generate_summary_report(insights)


        output_dir = '/opt/airflow/data/langchain_analysis'
        os.makedirs(output_dir, exist_ok=True)

        insights_path = f"{output_dir}/langchain_insights_{context['ds']}.json"
        with open(insights_path, 'w') as f:
            json.dump(insights, f, indent=2)

        report_path = f"{output_dir}/langchain_report_{context['ds']}.txt"
        with open(report_path, 'w') as f:
            f.write(summary_report)

        logger.info(f"✓ LangChain analysis completed")
        logger.info(f"  - Insights: {insights_path}")
        logger.info(f"  - Report: {report_path}")


        metrics = {
            'total_analyzed': len(insights),
            'high_priority': sum(1 for i in insights if i.get('response_priority') == 'high'),
            'requires_response': sum(1 for i in insights if i.get('requires_response', False)),
            'sentiment_distribution': {
                'positive': sum(1 for i in insights if i.get('sentiment', {}).get('sentiment_label') == 'positive'),
                'negative': sum(1 for i in insights if i.get('sentiment', {}).get('sentiment_label') == 'negative'),
                'neutral': sum(1 for i in insights if i.get('sentiment', {}).get('sentiment_label') == 'neutral')
            }
        }


        context['task_instance'].xcom_push(key='langchain_metrics', value=metrics)


        return metrics

    except Exception as e:
        logger.error(f"Error in LangChain analysis: {e}")

        raise


def basic_sentiment_analysis(**context):
    logger.info("Running basic sentiment analysis...")
    import random

    posts = [
        {'text': 'Great product!', 'brand': 'Apple'},
        {'text': 'Not happy with service', 'brand': 'Google'},
        {'text': 'Okay experience', 'brand': 'Microsoft'},
        {'text': 'Love this!', 'brand': 'Amazon'},
        {'text': 'Terrible quality', 'brand': 'Tesla'}
    ]

    results = []
    for post in posts:

        text_lower = post['text'].lower()

        if any(word in text_lower for word in ['great', 'love', 'amazing', 'excellent']):
            sentiment = 'positive'
            score = 0.8
        elif any(word in text_lower for word in ['terrible', 'bad', 'hate', 'worst']):
            sentiment = 'negative'
            score = -0.8
        else:
            sentiment = 'neutral'
            score = 0.0

        results.append({
            'text': post['text'],
            'brand': post['brand'],
            'sentiment': sentiment,
            'score': score
        })


    output_dir = '/opt/airflow/data/basic_analysis'
    os.makedirs(output_dir, exist_ok=True)

    results_path = f"{output_dir}/basic_sentiment_{context['ds']}.json"
    with open(results_path, 'w') as f:
        json.dump(results, f, indent=2)

    logger.info(f" Basic analysis completed: {results_path}")

    metrics = {
        'total_analyzed': len(results),
        'sentiment_distribution': {
            'positive': sum(1 for r in results if r['sentiment'] == 'positive'),
            'negative': sum(1 for r in results if r['sentiment'] == 'negative'),
            'neutral': sum(1 for r in results if r['sentiment'] == 'neutral')
        }
    }

    context['task_instance'].xcom_push(key='basic_metrics', value=metrics)

    return metrics


def generate_ai_insights_report(**context):

    use_langchain = context['task_instance'].xcom_pull(
        task_ids='check_langchain',
        key='use_langchain'
    )

    if use_langchain:
        metrics = context['task_instance'].xcom_pull(
            task_ids='langchain_sentiment_analysis',
            key='langchain_metrics'
        )
        analysis_type = 'LangChain AI Analysis'
    else:
        metrics = context['task_instance'].xcom_pull(
            task_ids='basic_sentiment_analysis',
            key='basic_metrics'
        )
        analysis_type = 'Basic Analysis'


    report = {
        'date': context['ds'],
        'analysis_type': analysis_type,
        'metrics': metrics,
        'summary': f"Analyzed {metrics['total_analyzed']} posts using {analysis_type}",
        'generated_at': datetime.now().isoformat()
    }


    output_dir = '/opt/airflow/data/reports'
    os.makedirs(output_dir, exist_ok=True)

    report_path = f"{output_dir}/ai_insights_report_{context['ds']}.json"
    with open(report_path, 'w') as f:
        json.dump(report, f, indent=2)

    logger.info(f"✓ AI insights report generated: {report_path}")

    return report


def identify_action_items(**context):

    use_langchain = context['task_instance'].xcom_pull(
        task_ids='check_langchain',
        key='use_langchain'
    )

    if not use_langchain:
        logger.info("Skipping action items (LangChain not available)")
        return {'action_items': []}

    insights_path = f"/opt/airflow/data/langchain_analysis/langchain_insights_{context['ds']}.json"

    try:
        with open(insights_path, 'r') as f:
            insights = json.load(f)


        action_items = []
        for insight in insights:
            if insight.get('requires_response') or insight.get('response_priority') == 'high':
                action_items.append({
                    'post_id': insight.get('post_id'),
                    'text': insight.get('original_text'),
                    'priority': insight.get('response_priority'),
                    'urgency': insight.get('urgency'),
                    'sentiment': insight.get('sentiment', {}).get('sentiment_label'),
                    'brands': [b['brand_name'] for b in insight.get('brands', [])],
                    'reason': insight.get('sentiment', {}).get('reasoning', '')
                })


        output_dir = '/opt/airflow/data/action_items'
        os.makedirs(output_dir, exist_ok=True)

        action_items_path = f"{output_dir}/action_items_{context['ds']}.json"
        with open(action_items_path, 'w') as f:
            json.dump(action_items, f, indent=2)

        logger.info(f" Identified {len(action_items)} action items")
        logger.info(f"  Saved to: {action_items_path}")

        return {'action_items': action_items, 'count': len(action_items)}

    except Exception as e:
        logger.error(f"Error identifying action items: {e}")
        return {'action_items': [], 'count': 0}


def send_priority_alerts(**context):

    action_items = context['task_instance'].xcom_pull(
        task_ids='identify_action_items'
    )
    count = action_items.get('count', 0)
    if count > 0:
        logger.warning(f" ALERT: {count} high-priority posts require attention!")

        for item in action_items.get('action_items', [])[:3]:
            logger.warning(f"  - {item['post_id']}: {item['text'][:100]}...")
    else:
        logger.info("✓ No high-priority alerts")



check_langchain = BranchPythonOperator(
    task_id='check_langchain',
    python_callable=check_langchain_available,
    dag=dag
)


langchain_analysis = PythonOperator(
    task_id='langchain_sentiment_analysis',
    python_callable=langchain_sentiment_analysis,
    dag=dag
)


basic_analysis = PythonOperator(
    task_id='basic_sentiment_analysis',
    python_callable=basic_sentiment_analysis,
    dag=dag
)


generate_report = PythonOperator(
    task_id='generate_ai_insights_report',
    python_callable=generate_ai_insights_report,
    trigger_rule='none_failed',
    dag=dag
)


action_items = PythonOperator(
    task_id='identify_action_items',
    python_callable=identify_action_items,
    trigger_rule='none_failed',
    dag=dag
)


priority_alerts = PythonOperator(
    task_id='send_priority_alerts',
    python_callable=send_priority_alerts,
    dag=dag
)


spark_batch_job = SparkSubmitOperator(
    task_id='spark_batch_processing',
    application='/opt/spark/work-dir/batch_sentiment_analysis.py',
    conn_id='spark_default',
    total_executor_cores=2,
    executor_cores=1,
    executor_memory='2g',
    driver_memory='1g',
    name='sentiment_batch_job',
    verbose=True,
    dag=dag
)



check_langchain >> [langchain_analysis, basic_analysis]
[langchain_analysis, basic_analysis] >> generate_report
generate_report >> action_items
action_items >> priority_alerts


check_langchain >> spark_batch_job