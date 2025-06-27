#!/usr/bin/env python3
"""
Synthetic Data Generator for Snowflake Cortex LLM Demo
Generates realistic synthetic data for customer interactions, product reviews, and support tickets
with correlations between datasets.

Usage:
    python generate_synthetic_data.py --num-records 100
"""

import argparse
import json
import random
import uuid
from datetime import datetime, timedelta
import os
from typing import List, Dict, Any, Tuple
from functools import lru_cache
import time
import multiprocessing
from concurrent.futures import ThreadPoolExecutor
import re

# For LLM text generation
import torch
from transformers import pipeline, set_seed

# Constants
LANGUAGES = ["english", "spanish", "french", "german", "italian", "portuguese"]
INTERACTION_TYPES = ["call", "email", "chat", "in-person", "social-media"]
TICKET_STATUSES = ["open", "in-progress", "resolved", "closed", "escalated"]
TICKET_CATEGORIES = ["technical", "billing", "account", "feature-request", "security", 
                      "product-question", "refund", "shipping", "compliance", "other"]
PRODUCT_CATEGORIES = ["software", "hardware", "service", "subscription", "consulting"]
SENTIMENT_PATTERNS = ["positive", "negative", "neutral", "mixed"]
PRODUCT_ISSUES = ["performance", "quality", "compatibility", "documentation", "pricing", "support"]

# Add more industry-specific and product-specific constants for variety
INDUSTRY_CONTEXTS = [
    "healthcare", "finance", "education", "retail", "manufacturing", "technology", 
    "entertainment", "government", "non-profit", "hospitality", "transportation",
    "energy", "agriculture", "construction", "real estate", "telecommunications"
]

PRODUCT_NAMES = [
    "DataSync Pro", "CloudGuard 365", "SecureConnect", "AnalyticsMaster", "IntegrateX",
    "DevOps Suite", "CollabPlatform", "MobileWorkspace", "CustomerInsight", "AutomationEngine",
    "IntelliDash", "SmartStack", "FileVault Pro", "NetworkDefender", "BusinessIQ",
    "RealTimeMonitor", "ProjectFlow", "DataHarbor", "AIAssistant", "SecureMessenger"
]

TECHNICAL_TERMS = [
    "API integration", "authentication protocol", "bandwidth utilization", "cache invalidation",
    "data migration", "encryption algorithm", "firewall configuration", "gateway timeout",
    "HTTP response code", "indexing performance", "JavaScript framework", "Kubernetes cluster",
    "load balancer", "microservice architecture", "NoSQL database", "OAuth token",
    "proxy server", "query optimization", "REST endpoint", "SSL certificate",
    "thread management", "UI rendering", "virtual machine", "webhook trigger",
    "XML parsing", "YAML configuration", "zero-downtime deployment"
]

CUSTOMER_CONCERNS = [
    "data privacy", "system reliability", "user permissions", "billing cycle",
    "feature limitations", "integration complexity", "performance bottleneck",
    "security vulnerability", "learning curve", "customer support response time",
    "customization options", "compliance requirements", "hidden costs",
    "platform stability", "mobile compatibility", "data export capabilities"
]

# List of fast, small models for text generation
FAST_MODELS = {
    "distilgpt2": {
        "description": "Small, fast distilled version of GPT-2 (82M parameters)"
    },
    "microsoft/MiniLM-L12-H384-uncased": {
        "description": "Small and fast model by Microsoft (33M parameters)"
    },
    "distilroberta-base": {
        "description": "Lightweight RoBERTa model (82M parameters)"
    },
    "facebook/opt-125m": {
        "description": "Tiny OPT model from Meta (125M parameters)"
    }
}

# Customer personas with consistent behavior patterns
CUSTOMER_PERSONAS = {
    "satisfied": {
        "review_rating_range": (4, 5),
        "interaction_tone": "positive",
        "support_frequency": "low",
        "loyalty": "high"
    },
    "frustrated": {
        "review_rating_range": (1, 2),
        "interaction_tone": "negative",
        "support_frequency": "high",
        "loyalty": "low"
    },
    "neutral": {
        "review_rating_range": (3, 4),
        "interaction_tone": "neutral",
        "support_frequency": "medium",
        "loyalty": "medium"
    },
    "mixed": {
        "review_rating_range": (2, 5),  # highly variable
        "interaction_tone": "mixed",
        "support_frequency": "medium",
        "loyalty": "medium"
    },
    "new": {
        "review_rating_range": (3, 5),
        "interaction_tone": "positive",
        "support_frequency": "medium",
        "loyalty": "unknown"
    }
}

# Pre-defined templates for faster text generation
TEMPLATES = {
    "positive_interaction": [
        "Customer was pleased with the solution provided. They appreciated the quick response and thoroughness. Will be upgrading their account next month.",
        "Very positive call with customer who praised our new features. They mentioned they've already recommended our product to colleagues.",
        "Customer reached out to express satisfaction with recent changes. They particularly liked the improved user interface and enhanced reporting capabilities.",
        "Great interaction with a happy customer. They praised the reliability of our product and mentioned they're planning to expand usage across their team.",
        "Customer was delighted with the quick resolution to their previous issue. They noted that our support is a key reason why they continue to use our product.",
        "Excellent follow-up call with customer. They complimented the product's reliability and ease of use.",
        "Customer mentioned how impressed they were with the recent updates. They're considering adding more users next quarter.",
        "Very productive call where the customer shared positive feedback about the integration capabilities.",
        "Customer enthusiastically reported their success using our advanced features. They achieved significant time savings.",
        "Proactive check-in call was well received. Customer is very satisfied with the service and had no issues to report."
    ],
    "negative_interaction": [
        "Customer expressed frustration about recurring technical issues. They mentioned considering competitor products if not resolved quickly.",
        "Difficult conversation with customer about billing discrepancies. They were upset about unexpected charges and requested immediate refund.",
        "Customer complained about poor performance and reliability issues. They need urgent resolution or will escalate to management.",
        "Customer was dissatisfied with recent changes to the interface. They found it confusing and less intuitive than the previous version.",
        "Frustrating call with customer reporting data loss issues. They were upset about the lack of warning before maintenance downtime.",
        "Customer called to express disappointment with response time on their support ticket. They expected faster resolution.",
        "Challenging discussion about system downtime. Customer emphasized the business impact and requested compensation.",
        "Customer was unhappy with the quality of documentation. They had difficulty implementing the solution without support.",
        "Call with unsatisfied customer who encountered multiple bugs in the latest release. They're considering downgrading.",
        "Customer voiced concerns about security features. They don't feel confident their data is adequately protected."
    ],
    "neutral_interaction": [
        "Routine check-in with customer. They had some questions about new features which were addressed during the call.",
        "Customer requested information about pricing for additional users. Sent documentation and scheduled follow-up for next week.",
        "Standard onboarding call with new customer. Walked through basic functionality and setup process.",
        "Customer needed help with account settings. Provided step-by-step instructions and confirmed they were able to complete the changes.",
        "Regular maintenance discussion with customer. Reviewed usage patterns and suggested optimization strategies.",
        "Follow-up call to verify implementation. Customer confirms setup is complete but hasn't fully tested all features.",
        "Provided customer with requested API documentation. They will review and get back with specific questions.",
        "Customer inquired about upcoming features. Shared roadmap information and approximate release dates.",
        "Routine account review with customer. Discussed current utilization and potential areas for improvement.",
        "Informational call about new compliance features. Customer will evaluate whether they need to implement them."
    ],
    "positive_review": [
        "I'm very satisfied with this product. It works exactly as described and the quality exceeds expectations. The customer service was also excellent when I had questions.",
        "This is an outstanding product that I would highly recommend. Easy to use, great functionality, and worth every penny.",
        "Excellent value for money. The features are comprehensive and the interface is intuitive. Very happy with my purchase.",
        "Five stars! This product has saved me hours of work each week. The automation features are particularly impressive.",
        "Couldn't be happier with my purchase. The setup was quick, performance is reliable, and it integrates well with my existing tools.",
        "Phenomenal product that has transformed our workflow. The learning curve was minimal and the benefits were immediate.",
        "Absolutely worth the investment. This tool has improved our team's productivity by at least 30% since implementation.",
        "Exceptional quality and reliability. In six months of heavy usage, we haven't encountered a single issue or bug.",
        "I've tried many similar products and this one stands out for its thoughtful design and powerful capabilities.",
        "Highly impressed with both the product and support team. They've been responsive and helpful throughout our onboarding.",
        "The best solution we've found after evaluating numerous alternatives. Meets all our requirements perfectly.",
        "Love how user-friendly this product is while still offering advanced features for power users.",
        "Game-changing tool that's become essential to our daily operations. Can't imagine working without it now.",
        "Superior performance compared to competitors. The speed and reliability make a noticeable difference.",
        "Great product with regular updates that actually improve functionality rather than just changing things around."
    ],
    "negative_review": [
        "Unfortunately, this product did not meet my expectations. There were quality issues and the functionality was limited compared to what was advertised.",
        "I'm disappointed with this purchase. The product arrived damaged and customer service was slow to respond to my concerns.",
        "Not worth the price. The product is difficult to use and lacks basic features I expected. Would not recommend.",
        "Frustrated with this product. It crashes frequently and loses data, which has caused significant problems for my work.",
        "Poor quality and unreliable. Save your money and look elsewhere for a better alternative.",
        "Regret purchasing this product. The learning curve is steep and the documentation is inadequate for troubleshooting.",
        "Constant performance issues make this tool practically unusable for our team. Support has been unhelpful.",
        "The interface is confusing and counterintuitive. Simple tasks require too many steps to complete.",
        "This product has numerous bugs that haven't been addressed despite multiple updates over several months.",
        "We experienced frequent downtime that impacted our business operations. Moving to a different solution.",
        "The pricing structure is misleading - many essential features require additional paid upgrades.",
        "Security concerns haven't been addressed adequately by the development team despite repeated inquiries.",
        "Poor compatibility with standard tools in our industry. Created more problems than it solved.",
        "Customer support is virtually non-existent. Waited weeks for responses to critical issues.",
        "The product lacks scalability. Works fine for small projects but breaks down with larger datasets."
    ],
    "neutral_review": [
        "The product works as expected. Nothing exceptional but does the job adequately. Fair value for the price point.",
        "Decent product with some good features. There's room for improvement but overall it meets basic needs.",
        "Average performance. Has some strengths and some weaknesses. Might work better for others depending on specific needs.",
        "It's okay. Does what it claims to do, though the interface could be more intuitive.",
        "Three stars. Works for basic tasks but lacks some advanced features that would make it exceptional.",
        "Functional but not particularly impressive. Meets minimum requirements for our needs.",
        "Middle-of-the-road solution that works reliably but doesn't stand out from competitors.",
        "Some features are well-designed while others feel incomplete. Regular updates have been gradually improving it.",
        "Acceptable performance for the price. There are better options if you're willing to pay more.",
        "Does what we need, though sometimes in a roundabout way. Documentation could be more comprehensive.",
        "Adequate solution that required some customization to fit our workflow. Support was helpful but slow.",
        "Reasonable choice for small teams. Larger organizations may find it lacks enterprise features.",
        "Mixed feelings about this product. Some departments love it while others find it limiting.",
        "Satisfactory performance overall. Neither disappointing nor particularly impressive.",
        "Basic functionality works well but advanced features aren't as polished as expected."
    ],
    "technical_issue": [
        "We're experiencing integration failures with the API. The error logs show connection timeouts when trying to process batch transactions.",
        "The dashboard is extremely slow to load and occasionally crashes when generating custom reports. This issue began after the latest update.",
        "Unable to export data in CSV format. The export process initiates but then fails with a generic error message.",
        "The search functionality is not returning accurate results. Specific keywords that should match content are showing zero results.",
        "System is automatically logging users out during active sessions, interrupting work and causing data loss.",
        "Encountering persistent memory leak when running extended analysis jobs. The application becomes increasingly slower until it crashes.",
        "Authentication system intermittently fails, preventing team members from accessing critical features during peak hours.",
        "Real-time data synchronization has stopped working between mobile and desktop applications. Changes aren't propagating correctly.",
        "Custom field configurations don't save properly when they contain special characters or exceed certain length limits.",
        "Notification system sending duplicate alerts and sometimes failing to deliver critical system warnings entirely."
    ],
    "billing_issue": [
        "Our account was charged twice for the annual subscription renewal. The duplicate payment needs to be refunded immediately.",
        "The latest invoice includes services we had previously canceled. We need this corrected and a revised invoice issued.",
        "We're being charged at the old rate despite switching to the new pricing tier last month.",
        "There appears to be a discrepancy between our usage metrics and the charges on our bill. We need clarification on how usage is calculated.",
        "We were promised a discount as part of our contract renewal, but it's not reflected in the recent billing statement.",
        "Payment method update failed multiple times on the portal. We've been trying to update our credit card information for two weeks.",
        "Unexpected price increase applied without prior notification. Our procurement department requires advance notice for budget planning.",
        "Tax exemption certificate was submitted last month but hasn't been applied to our account. We're still being charged tax incorrectly.",
        "Subscription auto-renewed despite our explicit request to cancel. Need immediate termination and prorated refund.",
        "Credits for service outage compensation haven't been applied to our account as promised by your support team."
    ],
    "account_issue": [
        "We need to add five new user accounts to our team but are having trouble with the permissions settings.",
        "Our admin account email needs to be updated following personnel changes in our organization.",
        "We're unable to deactivate former employee accounts through the admin dashboard. The deactivation option appears but doesn't save changes.",
        "Need help with setting up single sign-on for our organization to streamline the login process.",
        "Having difficulty managing team access levels. Need guidance on implementing role-based permissions.",
        "Two-factor authentication reset required for executive account after mobile device change. Previous backup codes were lost.",
        "Department restructuring requires bulk reassignment of users to different team hierarchies. Need assistance with the transition.",
        "Account merging request for two separate subscriptions our company maintains. We want to consolidate billing and user management.",
        "Custom domain verification failing despite following all the steps in the documentation. DNS records appear to be correctly configured.",
        "Request for audit logs of all admin actions over the past quarter for compliance review purposes."
    ],
    "feature_request": [
        "We would like to request an enhancement to the reporting functionality to include custom date ranges and export options.",
        "Our team needs the ability to bulk edit records. Currently making individual changes is time-consuming for large datasets.",
        "Request for enhanced notification settings that allow customization based on event priority and user preferences.",
        "We'd find it valuable to have an API endpoint for accessing historical usage data for integration with our internal analytics tools.",
        "Our team would benefit from having keyboard shortcuts for common actions to improve efficiency.",
        "Would like to see advanced filtering capabilities that allow for multiple conditions and saved filter presets.",
        "Request for white-labeling options so we can customize the interface with our company branding for client-facing portals.",
        "Need granular permission controls at the field level rather than just object-level permissions for sensitive data.",
        "Integration with project management tools like Jira and Asana would significantly improve our workflow.",
        "Enhanced data visualization options, particularly the ability to create custom dashboards with drag-and-drop components."
    ]
}

# Multilingual templates for non-English content
MULTILINGUAL_TEMPLATES = {
    "spanish": {
        "positive_review": [
            "Estoy muy satisfecho con este producto. Funciona exactamente como se describe y la calidad supera las expectativas. El servicio al cliente también fue excelente cuando tuve preguntas.",
            "Este es un producto excepcional que recomendaría ampliamente. Fácil de usar, gran funcionalidad, y vale cada centavo.",
            "Excelente relación calidad-precio. Las características son completas y la interfaz es intuitiva. Muy contento con mi compra.",
            "¡Cinco estrellas! Este producto me ha ahorrado horas de trabajo cada semana. Las funciones de automatización son particularmente impresionantes.",
            "No podría estar más feliz con mi compra. La configuración fue rápida, el rendimiento es confiable, y se integra bien con mis herramientas existentes.",
            "Producto fenomenal que ha transformado nuestro flujo de trabajo. La curva de aprendizaje fue mínima y los beneficios fueron inmediatos.",
            "Absolutamente vale la inversión. Esta herramienta ha mejorado la productividad de nuestro equipo en al menos un 30% desde su implementación.",
            "Calidad y fiabilidad excepcionales. En seis meses de uso intensivo, no hemos encontrado un solo problema o error.",
            "He probado muchos productos similares y este destaca por su diseño cuidadoso y sus potentes capacidades.",
            "Muy impresionado tanto con el producto como con el equipo de soporte. Han sido receptivos y útiles durante toda nuestra incorporación."
        ],
        "negative_review": [
            "Desafortunadamente, este producto no cumplió con mis expectativas. Había problemas de calidad y la funcionalidad era limitada en comparación con lo que se anunciaba.",
            "Estoy decepcionado con esta compra. El producto llegó dañado y el servicio al cliente tardó en responder a mis preocupaciones.",
            "No vale el precio. El producto es difícil de usar y carece de características básicas que esperaba. No lo recomendaría.",
            "Frustrado con este producto. Se bloquea con frecuencia y pierde datos, lo que ha causado problemas significativos para mi trabajo.",
            "Mala calidad y poco fiable. Ahorre su dinero y busque en otro lugar una mejor alternativa.",
            "Me arrepiento de haber comprado este producto. La curva de aprendizaje es empinada y la documentación es inadecuada para la resolución de problemas.",
            "Los constantes problemas de rendimiento hacen que esta herramienta sea prácticamente inutilizable para nuestro equipo. El soporte no ha sido útil.",
            "La interfaz es confusa y poco intuitiva. Las tareas simples requieren demasiados pasos para completarse.",
            "Este producto tiene numerosos errores que no se han solucionado a pesar de múltiples actualizaciones durante varios meses.",
            "Experimentamos frecuentes caídas del sistema que afectaron nuestras operaciones comerciales. Nos cambiamos a una solución diferente."
        ],
        "neutral_review": [
            "El producto funciona como se esperaba. Nada excepcional pero hace el trabajo adecuadamente. Justo valor por el precio.",
            "Producto decente con algunas buenas características. Hay margen de mejora pero en general satisface las necesidades básicas.",
            "Rendimiento promedio. Tiene algunas fortalezas y algunas debilidades. Podría funcionar mejor para otros dependiendo de necesidades específicas.",
            "Está bien. Hace lo que dice que hace, aunque la interfaz podría ser más intuitiva.",
            "Tres estrellas. Funciona para tareas básicas pero carece de algunas características avanzadas que lo harían excepcional.",
            "Funcional pero no particularmente impresionante. Cumple con los requisitos mínimos para nuestras necesidades.",
            "Solución intermedia que funciona de manera confiable pero no se destaca de los competidores.",
            "Algunas características están bien diseñadas mientras que otras se sienten incompletas. Las actualizaciones regulares lo han ido mejorando gradualmente.",
            "Rendimiento aceptable para el precio. Hay mejores opciones si estás dispuesto a pagar más.",
            "Hace lo que precisamos, anche si às vezes de maneira indireta. La documentação poderia ser mais abrangente."
        ]
    },
    "french": {
        "positive_review": [
            "Je suis très satisfait de ce produit. Il fonctionne exactement comme décrit et la qualité dépasse les attentes. Le service client était également excellent lorsque j'avais des questions.",
            "C'est un produit exceptionnel que je recommande vivement. Facile à utiliser, grande fonctionnalité et vaut chaque centime.",
            "Excellent rapport qualité-prix. Les fonctionnalités sont complètes et l'interface est intuitive. Très content de mon achat.",
            "Cinq étoiles! Ce produit m'a fait gagner des heures de travail chaque semaine. Les fonctionnalités d'automatisation sont particulièrement impressionnantes.",
            "Je ne pourrais pas être plus heureux de mon achat. La configuration a été rapide, les performances sont fiables et il s'intègre bien à mes outils existants.",
            "Produit phénoménal qui a transformé notre flux de travail. La courbe d'apprentissage était minimale et les avantages ont été immédiats.",
            "Absolument rentable. Cet outil a amélioré la productivité de notre équipe d'au moins 30% depuis sa mise en œuvre.",
            "Qualité et fiabilité exceptionnales. En six mois d'utilisation intensive, nous n'avons rencontré aucun problème ou bogue.",
            "J'ai essayé de nombreux produits similaires et celui-ci se démarque par sa conception réfléchie et ses capacités puissantes.",
            "Très impressionné par le produit et l'équipe de support. Ils ont été réactifs et utiles tout au long de notre intégration."
        ],
        "negative_review": [
            "Malheureusement, ce produit n'a pas répondu à mes attentes. Il y avait des problèmes de qualité et les fonctionnalités étaient limitées par rapport à ce qui était annoncé.",
            "Je suis déçu de cet achat. Le produit est arrivé endommagé et le service client a été lent à répondre à mes préoccupations.",
            "Ne vaut pas le prix. Le produit est difficile à utiliser et manque de fonctionnalités de base auxquelles je m'attendais. Je ne recommanderais pas.",
            "Frustré par ce produit. Il plante fréquemment et perd des données, ce qui a causé des problèmes importants pour mon travail.",
            "Mauvaise qualité et peu fiable. Économisez votre argent et cherchez ailleurs une meilleure alternative.",
            "Je regrette d'avoir acheté ce produit. La courbe d'apprentissage est abrupte et la documentation est inadéquate pour le dépannage.",
            "Des problèmes de performance constants rendent cet outil pratiquement inutilisable pour notre équipe. Le support n'a pas été utile.",
            "L'interface est déroutante et contre-intuitive. Les tâches simples nécessitent trop d'étapes pour être accomplies.",
            "Ce produit présente de nombreux bugs qui n'ont pas été corrigés malgré plusieurs mises à jour sur plusieurs mois.",
            "Nous avons connu de fréquentes interruptions de service qui ont impacté nos opérations commerciales. Nous passons à une solution différente."
        ],
        "neutral_review": [
            "Le produit fonctionne comme prévu. Rien d'exceptionnel mais fait le travail correctement. Juste valeur pour le prix.",
            "Produit correct avec quelques bonnes fonctionnalités. Il y a place à l'amélioration mais dans l'ensemble, il répond aux besoins de base.",
            "Performance moyenne. A des forces et des faiblesses. Pourrait mieux fonctionner pour d'autres selon les besoins spécifiques.",
            "C'est correct. Fait ce qu'il prétend faire, bien que l'interface pourrait être plus intuitive.",
            "Trois étoiles. Fonctionne pour les tâches de base mais manque de fonctionnalités avancées qui le rendraient exceptionnel.",
            "Fonctionnel mais pas particulièrement impressionnant. Répond aux exigences minimales pour nos besoins.",
            "Solution intermédiaire qui fonctionne de manière fiable mais ne se démarque pas des concurrents.",
            "Certaines fonctionnalités sont bien conçues tandis que d'autres semblent incomplètes. Des mises à jour régulières l'ont progressivement amélioré.",
            "Performance acceptable pour le prix. Il existe de meilleures options si vous êtes prêt à payer plus.",
            "Fait ce dont nous avons besoin, anche si às vezes de maneira détournée. La documentation pourrait être plus complète."
        ]
    },
    "german": {
        "positive_review": [
            "Ich bin mit diesem Produkt sehr zufrieden. Es funktioniert genau wie beschrieben und die Qualität übertrifft die Erwartungen. Der Kundenservice war auch ausgezeichnet, als ich Fragen hatte.",
            "Dies ist ein hervorragendes Produkt, das ich unbedingt empfehlen würde. Einfach zu bedienen, tolle Funktionalität und jeden Cent wert.",
            "Ausgezeichnetes Preis-Leistungs-Verhältnis. Die Funktionen sind umfassend und die Benutzeroberfläche ist intuitiv. Sehr glücklich mit meinem Kauf.",
            "Fünf Sterne! Dieses Produkt hat mir jede Woche Stunden Arbeit erspart. Die Automatisierungsfunktionen sind besonders beeindruckend.",
            "Könnte mit meinem Kauf nicht glücklicher sein. Die Einrichtung war schnell, die Leistung ist zuverlässig und es integriert sich gut mit meinen vorhandenen Tools.",
            "Phänomenales Produkt, das unseren Arbeitsablauf transformiert hat. Die Lernkurve war minimal und die Vorteile waren sofort spürbar.",
            "Absolut die Investition wert. Dieses Tool hat die Produktivität unseres Teams seit der Implementierung um mindestens 30% verbessert.",
            "Außergewöhnliche Qualität und Zuverlässigkeit. In sechs Monaten intensiver Nutzung haben wir kein einziges Problem oder keinen einzigen Fehler festgestellt.",
            "Ich habe viele ähnliche Produkte ausprobiert und dieses sticht durch sein durchdachtes Design und seine leistungsstarken Funktionen hervor.",
            "Sehr beeindruckt von sowohl dem Produkt als auch dem Support-Team. Sie waren während unseres gesamten Onboardings reaktionsschnell und hilfreich."
        ],
        "negative_review": [
            "Leider hat dieses Produkt meine Erwartungen nicht erfüllt. Es gab Qualitätsprobleme und die Funktionalität war im Vergleich zu dem, was beworben wurde, eingeschränkt.",
            "Ich bin von diesem Kauf enttäuscht. Das Produkt kam beschädigt an und der Kundenservice reagierte nur langsam auf meine Bedenken.",
            "Nicht den Preis wert. Das Produkt ist schwer zu bedienen und es fehlen grundlegende Funktionen, die ich erwartet hatte. Würde ich nicht empfehlen.",
            "Frustriert mit diesem Produkt. Es stürzt häufig ab und verliert Daten, was zu erheblichen Problemen bei meiner Arbeit geführt hat.",
            "Schlechte Qualität und unzuverlässig. Sparen Sie Ihr Geld und suchen Sie woanders nach einer besseren Alternative.",
            "Ich bereue den Kauf dieses Produkts. Die Lernkurve ist steil und die Dokumentation ist für die Fehlerbehebung unzureichend.",
            "Ständige Leistungsprobleme machen dieses Tool für unser Team praktisch unbrauchbar. Der Support war nicht hilfreich.",
            "Die Benutzeroberfläche ist verwirrend und kontraintuitiv. Einfache Aufgaben erfordern zu viele Schritte, um sie zu erledigen.",
            "Dieses Produkt hat zahlreiche Fehler, die trotz mehrerer Updates über mehrere Monate hinweg nicht behoben wurden.",
            "Wir erlebten häufige Ausfallzeiten, die unsere Geschäftsabläufe beeinträchtigten. Wir wechseln zu einer anderen Lösung."
        ],
        "neutral_review": [
            "Das Produkt funktioniert wie erwartet. Nichts Außergewöhnliches, aber erledigt die Aufgabe angemessen. Fairer Wert für den Preis.",
            "Anständiges Produkt mit einigen guten Funktionen. Es gibt Raum für Verbesserungen, aber insgesamt erfüllt es grundlegende Bedürfnisse.",
            "Durchschnittliche Leistung. Hat einige Stärken und einige Schwächen. Könnte für andere je nach spezifischen Bedürfnissen besser funktionieren.",
            "Es ist in Ordnung. Tut, was es zu tun behauptet, obwohl die Benutzeroberfläche intuitiver sein könnte.",
            "Drei Sterne. Funktioniert für grundlegende Aufgaben, aber es fehlen einige fortgeschrittene Funktionen, die es außergewöhnlich machen würden.",
            "Funktional, aber nicht besonders beeindruckend. Erfüllt die Mindestanforderungen für unsere Bedürfnisse.",
            "Mittelmäßige Lösung, die zuverlässig funktioniert, sich aber nicht von Konkurrenten abhebt.",
            "Einige Funktionen sind gut gestaltet, während andere unvollständig wirken. Regelmäßige Updates haben es nach und nach verbessert.",
            "Akzeptable Leistung für den Preis. Es gibt bessere Optionen, wenn Sie bereit sind, mehr zu bezahlen.",
            "Macht, was wir brauchen, wenn auch manchmal auf Umwegen. Die Dokumentation könnte umfassender sein."
        ]
    },
    "italian": {
        "positive_review": [
            "Sono molto soddisfatto di questo prodotto. Funziona esattamente come descritto e la qualità supera le aspettative. Anche il servizio clienti è stato eccellente quando avevo domande.",
            "Questo è un prodotto eccezionale che consiglierei vivamente. Facile da usare, ottime funzionalità e vale ogni centesimo.",
            "Eccellente rapporto qualità-prezzo. Le caratteristiche sono complete e l'interfaccia è intuitiva. Molto felice del mio acquisto.",
            "Cinque stelle! Questo prodotto mi ha fatto risparmiare ore di lavoro ogni settimana. Le funzionalità di automazione sono particolarmente impressionanti.",
            "Non potrei essere più felice del mio acquisto. La configurazione è stata rapida, le prestazioni sono affidabili e si integra bene con i miei strumenti esistenti.",
            "Prodotto fenomenale che ha trasformato il nostro flusso di lavoro. La curva di apprendimento è stata minima e i benefici sono stati immediati.",
            "Assolutamente vale l'investimento. Questo strumento ha migliorato la produttività del nostro team di almeno il 30% dall'implementazione.",
            "Qualità e affidabilità eccezionali. In sei mesi di utilizzo intenso, non abbiamo riscontrato un singolo problema o bug.",
            "Ho provato molti prodotti simili e questo spicca per il suo design attento e le potenti funzionalità.",
            "Molto colpito sia dal prodotto che dal team di supporto. Sono stati reattivi e disponibili durante tutto il nostro onboarding."
        ],
        "negative_review": [
            "Purtroppo, questo prodotto non ha soddisfatto le mie aspettative. C'erano problemi di qualità e la funzionalità era limitata rispetto a quanto pubblicizzato.",
            "Sono deluso di questo acquisto. Il prodotto è arrivato danneggiato e il servizio clienti è stato lento a rispondere alle mie preoccupazioni.",
            "Non vale il prezzo. Il prodotto è difficile da usare e manca di funzionalità di base che mi aspettavo. Non lo consiglierei.",
            "Frustrato con questo prodotto. Si blocca frequentemente e perde dati, il che ha causato problemi significativi per il mio lavoro.",
            "Scarsa qualità e inaffidabile. Risparmia i tuoi soldi e cerca altrove un'alternativa migliore.",
            "Mi pento di aver acquistato questo prodotto. La curva di apprendimento è ripida e la documentazione è inadeguata per la risoluzione dei problemi.",
            "Costanti problemi di prestazioni rendono questo strumento praticamente inutilizzabile for il nostro team. Il supporto non è stato utile.",
            "L'interfaccia è confusa e controintuitiva. Le attività semplici richiedono troppi passaggi per essere completate.",
            "Questo prodotto ha numerosi bug che non sono stati risolti nonostante diversi aggiornamenti nell'arco di diversi mesi.",
            "Abbiamo riscontrato frequenti interruzioni che hanno influito sulle nostre operazioni aziendali. Stiamo passando a una soluzione diversa."
        ],
        "neutral_review": [
            "Il prodotto funziona come previsto. Niente di eccezionale ma svolge il lavoro adeguatamente. Giusto valore per il prezzo.",
            "Prodotto decente con alcune buone caratteristiche. C'è margine di miglioramento ma nel complesso soddisfa le esigenze di base.",
            "Prestazioni nella media. Ha alcuni punti di forza e alcune debolezze. Potrebbe funzionare meglio per altri a seconda delle esigenze specifiche.",
            "È accettabile. Fa ciò che dice di fare, anche se l'interfaccia potrebbe essere più intuitiva.",
            "Tre stelle. Funziona for attività di base ma manca di alcune funzionalità avanzate che lo renderebbero eccezionale.",
            "Funzionale ma non particularmente impressionante. Soddisfa i requisiti minimi per le nostre esigenze.",
            "Soluzione di medio livello che funziona in modo affidabile ma non si distingue dai concorrenti.",
            "Alcune funzionalità sono ben progettate mentre altre sembrano incomplete. Aggiornamenti regolari têm migliorato gradualmente.",
            "Prestazioni accettabili per il prezzo. Ci sono opzioni migliori se sei disposto a pagare di più.",
            "Faz o que precisamos, anche se às vezes de maneira indireta. La documentação poderia ser mais abrangente."
        ]
    },
    "portuguese": {
        "positive_review": [
            "Estou muito satisfeito com este produto. Funciona exatamente como descrito e a qualidade excede as expectativas. O atendimento ao cliente também foi excelente quando tive dúvidas.",
            "Este é um produto excepcional que eu recomendaria altamente. Fácil de usar, ótima funcionalidade e vale cada centavo.",
            "Excelente custo-benefício. Os recursos são abrangentes e a interface é intuitiva. Muito feliz com minha compra.",
            "Cinco estrelas! Este produto me economizou horas de trabalho por semana. Os recursos de automação são particularmente impressionantes.",
            "Não poderia estar mais feliz com minha compra. A configuração foi rápida, o desempenho é confiável e se integra bem com minhas ferramentas existentes.",
            "Produto fenomenal que transformou nosso fluxo de trabalho. A curva de aprendizado foi mínima e os benefícios foram imediatos.",
            "Absolutamente vale o investimento. Esta ferramenta melhorou a produtividade da nossa equipe em pelo menos 30% desde a implementação.",
            "Qualidade e confiabilidade excepcionais. Em seis meses de uso intenso, não encontramos um único problema ou bug.",
            "Experimentei muitos produtos semelhantes e este se destaca pelo seu design cuidadoso e recursos poderosos.",
            "Muito impressionado com o produto e com a equipe de suporte. Eles foram responsivos e prestativos durante todo o nosso processo de integração."
        ],
        "negative_review": [
            "Infelizmente, este produto não atendeu às minhas expectativas. Houve problemas de qualité e a funcionalidade era limitada em comparação com o que foi anunciado.",
            "Estou decepcionado com esta compra. O produto chegou danificado e o atendimento ao cliente foi lento para responder às minhas preocupações.",
            "Não vale o preço. O produto é difícil de usar e não possui recursos básicos que eu esperava. Não recomendaria.",
            "Frustrado com este produto. Ele trava frequentemente e perde dados, o que causou problemas significativos para o meu trabalho.",
            "Má qualidade e não confiável. Economize seu dinheiro e procure em outro lugar uma alternativa melhor.",
            "Me arrependo de comprar este produto. A curva de aprendizado é íngreme e a documentação é inadequada para solução de problemas.",
            "Problemas constantes de desempenho tornam esta ferramenta praticamente inutilizável para nossa equipe. O suporte não foi útil.",
            "A interface é confusa e contra-intuitiva. Tarefas simples exigem muitas etapas para serem concluídas.",
            "Este produto tem vários bugs que não foram corrigidos apesar de várias atualizações ao longo de vários meses.",
            "Experimentamos frequentes períodos de inatividade que afetaram nossas operações comerciais. Estamos mudando para uma solução diferente."
        ],
        "neutral_review": [
            "O produto funciona como esperado. Nada excepcional, mas faz o trabalho adequadamente. Valor justo para o preço.",
            "Produto decente com alguns bons recursos. Há espaço para melhorias, mas no geral atende às necessidades básicas.",
            "Desempenho médio. Tem alguns pontos fortes e alguns pontos fracos. Pode funcionar melhor para outros, dependendo das necessidades específicas.",
            "Está ok. Faz o que diz que faz, embora a interface pudesse ser mais intuitiva.",
            "Três estrelas. Funciona para tarefas básicas, mas carece de alguns recursos avançados que o tornariam excepcional.",
            "Funcional, mas não particularmente impressionante. Atende aos requisitos mínimos para nossas esigenzes.",
            "Solução mediana que funciona de forma confiável, mas não se destaca dos concorrentes.",
            "Alguns recursos são bem projetados, enquanto outros parecem incompletos. Atualizações regulares têm melhorado gradualmente.",
            "Desempenho aceitável pelo preço. Existem opções melhores se você estiver disposto a pagar mais.",
            "Faz o que precisamos, embora às vezes de maneira indireta. A documentação poderia ser mais abrangente."
        ]
    }
}

# Translation dictionaries for common phrases in reviews
TRANSLATIONS = {
    "spanish": {
        "very satisfied": "muy satisfecho",
        "excellent": "excelente",
        "outstanding": "excepcional",
        "recommend": "recomendar",
        "quality": "calidad",
        "easy to use": "fácil de usar",
        "value for money": "relación calidad-precio",
        "happy": "feliz",
        "disappointed": "decepcionado",
        "not worth": "no vale",
        "poor quality": "mala calidad",
        "unfortunately": "desafortunadamente",
        "frustrated": "frustrado",
        "decent": "decente",
        "average": "promedio",
        "works as expected": "funciona como se esperaba",
    },
    "french": {
        "very satisfied": "très satisfait",
        "excellent": "excellent",
        "outstanding": "exceptionnel",
        "recommend": "recommander",
        "quality": "qualité",
        "easy to use": "facile à utiliser",
        "value for money": "rapport qualité-prix",
        "happy": "content",
        "disappointed": "déçu",
        "not worth": "ne vaut pas",
        "poor quality": "mauvaise qualité",
        "unfortunately": "malheureusement",
        "frustrated": "frustré",
        "decent": "correct",
        "average": "moyen",
        "works as expected": "fonctionne comme prévu",
    },
    "german": {
        "very satisfied": "sehr zufrieden",
        "excellent": "ausgezeichnet",
        "outstanding": "hervorragend",
        "recommend": "empfehlen",
        "quality": "Qualität",
        "easy to use": "einfach zu bedienen",
        "value for money": "Preis-Leistungs-Verhältnis",
        "happy": "glücklich",
        "disappointed": "enttäuscht",
        "not worth": "nicht wert",
        "poor quality": "schlechte Qualität",
        "unfortunately": "leider",
        "frustrated": "frustriert",
        "decent": "anständig",
        "average": "durchschnittlich",
        "works as expected": "funktioniert wie erwartet",
    },
    "italian": {
        "very satisfied": "molto soddisfatto",
        "excellent": "eccellente",
        "outstanding": "eccezionale",
        "recommend": "raccomandare",
        "quality": "qualità",
        "easy to use": "facile da usare",
        "value for money": "rapporto qualità-prezzo",
        "happy": "felice",
        "disappointed": "deluso",
        "not worth": "non vale",
        "poor quality": "scarsa qualità",
        "unfortunately": "purtroppo",
        "frustrated": "frustrato",
        "decent": "decente",
        "average": "nella media",
        "works as expected": "funziona come previsto",
    },
    "portuguese": {
        "very satisfied": "muito satisfeito",
        "excellent": "excelente",
        "outstanding": "excepcional",
        "recommend": "recomendar",
        "quality": "qualidade",
        "easy to use": "fácil de usar",
        "value for money": "custo-benefício",
        "happy": "feliz",
        "disappointed": "decepcionado",
        "not worth": "não vale",
        "poor quality": "má qualidade",
        "unfortunately": "infelizmente",
        "frustrated": "frustrado",
        "decent": "decente",
        "average": "médio",
        "works as expected": "funciona como esperado",
    }
}

def translate_text(text, target_language):
    """Translate text by replacing key phrases with their translations."""
    if target_language.lower() == "english" or target_language.lower() not in TRANSLATIONS:
        return text
        
    translated_text = text
    translation_dict = TRANSLATIONS[target_language.lower()]
    
    # Replace phrases with their translations (case-insensitive)
    for eng_phrase, translated_phrase in translation_dict.items():
        pattern = re.compile(re.escape(eng_phrase), re.IGNORECASE)
        translated_text = pattern.sub(translated_phrase, translated_text)
    
    # If the text hasn't been modified by any translations, use a generic translated template
    if translated_text == text:
        generic_translations = {
            "spanish": {
                "positive": "Excelente producto con muy buenas características.",
                "negative": "Producto decepcionante con varios problemas.",
                "neutral": "Producto básico que cumple su función."
            },
            "french": {
                "positive": "Excellent produit avec de très bonnes fonctionnalités.",
                "negative": "Produit décevant avec plusieurs problèmes.",
                "neutral": "Produit basique qui remplit sa fonction."
            },
            "german": {
                "positive": "Ausgezeichnetes Produkt mit sehr guten Funktionen.",
                "negative": "Enttäuschendes Produkt mit mehreren Problemen.",
                "neutral": "Grundlegendes Produkt, das seine Funktion erfüllt."
            },
            "italian": {
                "positive": "Eccellente prodotto con ottime funzionalità.",
                "negative": "Prodotto deludente con diversi problemi.",
                "neutral": "Prodotto base che svolge la sua funzione."
            },
            "portuguese": {
                "positive": "Excelente produto com ótimas funcionalidades.",
                "negative": "Produto decepcionante com vários problemas.",
                "neutral": "Produto básico que cumpre sua função."
            }
        }
        
        # Determine the sentiment from the text
        sentiment = "neutral"
        if "excellent" in text.lower() or "great" in text.lower() or "good" in text.lower():
            sentiment = "positive"
        elif "poor" in text.lower() or "bad" in text.lower() or "disappointed" in text.lower():
            sentiment = "negative"
            
        # Use the generic translation if available
        if target_language in generic_translations and sentiment in generic_translations[target_language]:
            translated_text = generic_translations[target_language][sentiment]
    
    return translated_text

def setup_llm(use_fallback_only=False, model_name="distilgpt2"):
    """Setup the text generation pipeline using a small open-source LLM."""
    if use_fallback_only:
        return None
        
    # Use a smaller model that can run locally
    try:
        print(f"Loading LLM model '{model_name}' (this may take a moment)...")
        
        # Determine the best device to use
        if torch.cuda.is_available():
            device = "cuda"
            dtype = torch.float16
        elif torch.backends.mps.is_available():
            device = "mps"
            dtype = torch.float32
        else:
            device = "cpu"
            dtype = torch.float32
            
        print(f"Using device: {device}")
        
        generator = pipeline('text-generation', 
                            model=model_name,
                            torch_dtype=dtype,
                            device_map=device)
        return generator
    except Exception as e:
        print(f"Failed to load LLM: {e}")
        print("Falling back to template-based text generation")
        return None

def generate_text_with_llm(generator, prompt_key, tone=None, language="english", max_length: int = 150, **context_params):
    """Generate text using the LLM or templates with caching."""
    # For non-English content, always use templates to ensure proper translation
    if language.lower() != "english":
        return generate_from_template(prompt_key, tone, language)
    
    # If template ratio is set, randomly decide whether to use templates
    if random.random() < TEMPLATE_RATIO:
        return generate_from_template(prompt_key, tone, language)
    
    # If no generator is available, fall back to templates
    if generator is None:
        return generate_from_template(prompt_key, tone, language)
    
    try:
        # Get context parameters
        context = context_params.get("context", "")
        scenario = context_params.get("scenario", "")
        product = context_params.get("product", "")
        
        # Create more varied prompts with specifics to increase diversity
        # Select from curated lists to create variety without overwhelming memory
        contexts = [
            "tech startup", "retail store", "financial service", "healthcare provider", 
            "educational platform", "mobile app", "subscription service", "e-commerce site"
        ]
        
        products = [
            "software subscription", "hardware device", "mobile application", "API service",
            "data analytics tool", "smart device", "cloud storage", "security solution"
        ] + PRODUCT_NAMES[:5]  # Just use a few product names
        
        scenarios = [
            "after an upgrade", "during onboarding", "while troubleshooting", "regarding billing",
            "about feature requests", "during peak usage", "following system outage"
        ]
        
        # Select random elements to create more varied context
        context = random.choice(contexts)
        product = random.choice(products)
        scenario = random.choice(scenarios)
        
        # Create appropriate detailed prompt based on the key
        if prompt_key == "interaction":
            # Generate random enrichment details for more variety
            enrichment = {
                "time_period": random.choice(["recent", "yesterday's", "last week's", "this morning's", ""]),
                "duration": random.choice(["brief", "lengthy", "30-minute", "hour-long", "multi-session", ""]),
                "channel": random.choice(["phone", "email", "chat", "video call", "in-person", "social media", ""]),
                "customer_detail": random.choice(["new", "long-term", "technical", "non-technical", "enterprise", "small business", ""]),
                "specific_concern": random.choice(CUSTOMER_CONCERNS + ["", "", "", ""]),  # Sometimes blank for variety
                "usage_detail": random.choice(["heavy user", "occasional user", "first-time user", "power user", ""])
            }
            
            prompt_templates = [
                f"Write a {tone} customer service interaction note for a {enrichment['customer_detail']} {context} {scenario} via {enrichment['channel']}:",
                f"Create a detailed {tone} support interaction summary for a {enrichment['usage_detail']} customer using our {product} who mentioned {enrichment['specific_concern']}:",
                f"Draft a {tone} customer call transcript from {enrichment['time_period']} {enrichment['duration']} conversation {scenario} with specific details:",
                f"Document a {tone} {enrichment['channel']} conversation between our support team and a {context} customer who {scenario}:",
                f"Prepare a comprehensive {tone} interaction record for a {enrichment['customer_detail']} {context} client with concerns about {product}:",
                f"Summarize a {tone} {enrichment['duration']} live chat session with a customer who needed help with {product} related to {enrichment['specific_concern']}:",
                f"Record a {tone} {enrichment['channel']} interaction with a {enrichment['usage_detail']} exploring our {product} features:",
                f"Create notes from a {tone} {enrichment['time_period']} follow-up call with a {context} customer regarding their previous {scenario}:"
            ]
            prompt = random.choice(prompt_templates)
        elif prompt_key == "review":
            # Generate random enrichment details for more variety
            enrichment = {
                "usage_duration": random.choice(["after one week of using", "after a month with", "after 6 months of", "after a year with", ""]),
                "purchase_reason": random.choice(["purchased for business use", "bought for personal projects", "selected after comparing alternatives", "upgraded from previous version", ""]),
                "specific_feature": random.choice(["automation capabilities", "user interface", "mobile app", "integration options", "reporting tools", "customer support", ""]),
                "comparison": random.choice(["compared to competitors", "compared to previous versions", "against similar products", "against my expectations", ""]),
                "use_case": random.choice(["for daily operations", "for occasional tasks", "for critical business processes", "for personal productivity", ""])
            }
            
            prompt_templates = [
                f"Write a detailed {tone} product review for a {product} from a {context} perspective {enrichment['usage_duration']} it {enrichment['use_case']}:",
                f"Create a specific {tone} customer review for our {product} with personal experience details, particularly about the {enrichment['specific_feature']}:",
                f"Draft a {tone} user review for a {product} mentioning specific features {enrichment['comparison']}:",
                f"Compose an authentic {tone} review from a {context} customer who's been using {product} for several months {enrichment['use_case']}:",
                f"Write a {tone} testimonial highlighting how {product}'s {enrichment['specific_feature']} affected a customer's workflow:",
                f"Generate a {tone} product rating with specific examples of {product} performance {enrichment['comparison']}:",
                f"Create a {tone} comparison review between our {product} and competitor alternatives focusing on {enrichment['specific_feature']}:",
                f"Draft a detailed {tone} review by someone who {enrichment['purchase_reason']} focusing on the value-for-money aspect of our {product}:"
            ]
            prompt = random.choice(prompt_templates)
        elif prompt_key == "ticket":
            # Generate random enrichment details for more variety
            enrichment = {
                "urgency": random.choice(["urgent", "critical", "moderate", "low-priority", ""]),
                "reproduction": random.choice(["consistently reproducible", "intermittent", "only under specific conditions", "random", ""]),
                "impact": random.choice(["blocking workflow", "causing delays", "affecting data integrity", "creating user confusion", "reducing performance", ""]),
                "environment": random.choice(["production", "development", "testing", "staging", "mobile", "desktop", ""]),
                "steps_taken": random.choice(["after trying basic troubleshooting", "after consulting documentation", "after system restart", "after clearing cache", ""]),
                "technical_detail": random.choice(TECHNICAL_TERMS + ["", "", ""])  # Sometimes blank for variety
            }
            
            prompt_templates = [
                f"Write a detailed {enrichment['urgency']} customer support ticket about a {tone} issue with our {product} {scenario} in the {enrichment['environment']} environment:",
                f"Create a specific {tone} support request from a {context} customer with technical details about {enrichment['technical_detail']} {enrichment['impact']}:",
                f"Draft a thorough {tone} customer ticket explaining a {enrichment['reproduction']} problem with our {product} {enrichment['steps_taken']}:",
                f"Document a {tone} help desk ticket from a {context} user unable to complete a task with {product} due to an issue {enrichment['impact']}:",
                f"Write a {enrichment['urgency']} {tone} bug report submitted by a customer using our {product} in an enterprise environment with details about {enrichment['technical_detail']}:",
                f"Generate a {tone} feature request ticket with specific use case examples for our {product} related to {enrichment['technical_detail']}:",
                f"Create a detailed {tone} troubleshooting ticket where a customer tried several solutions for their {product} issue that is {enrichment['reproduction']}:",
                f"Compose a {tone} escalation ticket from a {context} customer who has had recurring issues with {product} {enrichment['impact']}:"
            ]
            prompt = random.choice(prompt_templates)
        else:
            # Default fallback
            return generate_from_template(prompt_key, tone, language)
            
        # Use a random seed each time for more variety
        set_seed(random.randint(1, 10000))
        
        # Adjust generation parameters for variety but avoid memory issues
        # Use moderate temperature for variety without excessive computation
        temperature = random.uniform(0.7, 1.3)  # Increased upper range for more variety
        top_p = random.uniform(0.7, 0.99)  # Wider range for more diverse outputs
        top_k = random.randint(40, 100)  # Add top_k sampling for even more variety
        
        generation_params = {
            "max_length": max_length + random.randint(10, 50),  # Increased length variation
            "truncation": True,
            "do_sample": True,
            "temperature": temperature,
            "top_p": top_p,
            "top_k": top_k,  # Add top_k parameter
            "repetition_penalty": random.uniform(1.1, 1.3),  # Add repetition penalty
            "num_return_sequences": 1
        }
        
        # Run generation with appropriate parameters
        result = generator(prompt, **generation_params)
        
        # Extract the generated text - this handles both model types (dict vs list output)
        if isinstance(result, list) and len(result) > 0:
            if isinstance(result[0], dict) and 'generated_text' in result[0]:
                # Format for models like GPT-2, DistilGPT2
                generated_text = result[0]['generated_text']
                # Remove the prompt from the beginning
                if generated_text.startswith(prompt):
                    generated_text = generated_text[len(prompt):].strip()
            else:
                # Handle other list formats
                generated_text = result[0]
        else:
            # Other model formats or unexpected output
            if isinstance(result, str):
                generated_text = result
            else:
                # Fallback if we can't parse the result
                return generate_from_template(prompt_key, tone, language)
        
        # Post-process to make sure it's useful text
        # Ensure it's not empty or too short
        if len(generated_text) < 10:
            return generate_from_template(prompt_key, tone, language)
            
        # Normalize text by removing excess whitespace
        generated_text = ' '.join(generated_text.split())
        
        # For non-English content, use templates to ensure proper translation
        if language.lower() != "english":
            return generate_from_template(prompt_key, tone, language)
        
        return generated_text
    except Exception as e:
        print(f"Text generation error: {e}")
        return generate_from_template(prompt_key, tone, language)

def get_multilingual_template(template_key, language):
    """Get a template in the specified language if available, or fallback to English."""
    language = language.lower()
    
    # Extract the sentiment part (positive, negative, neutral)
    sentiment = None
    if template_key.startswith("positive"):
        sentiment = "positive"
    elif template_key.startswith("negative"):
        sentiment = "negative"
    elif template_key.startswith("neutral"):
        sentiment = "neutral"
    
    # Only product reviews are currently translated
    if sentiment and language in MULTILINGUAL_TEMPLATES:
        if f"{sentiment}_review" in MULTILINGUAL_TEMPLATES[language]:
            return random.choice(MULTILINGUAL_TEMPLATES[language][f"{sentiment}_review"])
    
    # If no matching template is found, translate an English template
    if template_key in TEMPLATES:
        english_text = random.choice(TEMPLATES[template_key])
        translated_text = translate_text(english_text, language)
        return translated_text
    
    return None

def generate_from_template(prompt_key, tone=None, language="english"):
    """Generate text using pre-defined templates with language translation."""
    template_key = None
    
    if prompt_key == "interaction":
        template_key = f"{tone}_interaction"
    elif prompt_key == "review":
        template_key = f"{tone}_review"
    elif prompt_key == "ticket":
        if tone in ["technical", "billing", "account", "feature-request"]:
            template_key = f"{tone}_issue"
        else:
            # For other ticket categories, use a random issue type
            template_key = random.choice(["technical_issue", "billing_issue", "account_issue", "feature_request"])
    
    # For non-English reviews, use multilingual templates
    if language.lower() != "english":
        if prompt_key == "review" and tone in ["positive", "negative", "neutral"]:
            multilingual_text = get_multilingual_template(f"{tone}_review", language)
            if multilingual_text:
                return multilingual_text
    
    # For other templates, use regular templates with translation if needed
    if template_key in TEMPLATES:
        text = random.choice(TEMPLATES[template_key])
        # Translate for non-English content
        if language.lower() != "english":
            return translate_text(text, language)
        return text
    
    # Fallback generic templates
    generic_templates = [
        "This matter requires attention. Please review the details and take appropriate action.",
        "I would like to discuss this at your earliest convenience. Several aspects need clarification.",
        "Thank you for your assistance with this situation. The information provided has been helpful."
    ]
    text = random.choice(generic_templates)
    
    # Translate for non-English content
    if language.lower() != "english":
        return translate_text(text, language)
    return text

def generate_customer_base(num_records: int) -> List[Dict[str, Any]]:
    """Generate a consistent customer base with personas."""
    customers = []
    
    for i in range(1, num_records + 1):
        customer_id = f"CUST-{i:03d}"
        # Assign a persona to each customer
        persona = random.choice(list(CUSTOMER_PERSONAS.keys()))
        
        # Generate some basic customer data
        customer = {
            "customer_id": customer_id,
            "persona": persona,
            "sign_up_date": (datetime.now() - timedelta(days=random.randint(30, 1095))).strftime("%Y-%m-%d"),
            "products_owned": random.randint(1, 5),
            "lifetime_value": random.randint(100, 10000)
        }
        customers.append(customer)
    
    return customers

def generate_customer_interactions(customers, generator, use_parallel=False) -> List[Dict[str, Any]]:
    """Generate realistic customer interactions."""
    interactions = []
    customer_ids = [customer["customer_id"] for customer in customers]
    
    # More customers should have more interactions
    num_interactions = min(len(customers) * 3, 1000)  # Cap at 1000
    
    # About 60% positive, 30% negative, 10% neutral
    positive_count = int(num_interactions * 0.6)
    negative_count = int(num_interactions * 0.3)
    neutral_count = num_interactions - positive_count - negative_count
    
    # Generate batches of interactions
    items_to_process = []
    
    # First add positive interactions - frequent customers
    frequent_customers = customer_ids[:int(len(customer_ids) * 0.4)]
    for _ in range(positive_count):
        customer_id = random.choice(frequent_customers)
        date = generate_random_date()
        items_to_process.append({
            'customer_id': customer_id,
            'date': date,
            'tone': 'positive'
        })
        
    # Then negative interactions - add to remaining customers
    problem_customers = customer_ids[int(len(customer_ids) * 0.6):]
    for _ in range(negative_count):
        customer_id = random.choice(problem_customers)
        date = generate_random_date()
        items_to_process.append({
            'customer_id': customer_id,
            'date': date,
            'tone': 'negative'
        })
    
    # Finally neutral interactions - spread across all customers
    for _ in range(neutral_count):
        customer_id = random.choice(customer_ids)
        date = generate_random_date()
        items_to_process.append({
            'customer_id': customer_id,
            'date': date,
            'tone': 'neutral'
        })
    
    # Process the items
    if use_parallel:
        interactions = batch_process_generation(items_to_process, generator, process_interaction_item)
    else:
        for item in items_to_process:
            interaction = process_interaction_item(item, generator)
            interactions.append(interaction)
    
    # Sort by date for realistic timeline
    interactions.sort(key=lambda x: x["interaction_date"])
    
    # Generate sequential IDs
    for i, interaction in enumerate(interactions):
        interaction["interaction_id"] = f"INT-{i+1:03d}"
    
    return interactions

def generate_product_reviews(customers, generator, use_parallel=False) -> List[Dict[str, Any]]:
    """Generate product reviews with sentiment matching customer profile."""
    reviews = []
    customer_ids = [customer["customer_id"] for customer in customers]
    
    # More customers should have more reviews
    num_reviews = min(len(customers) * 2, 1000)  # Cap at 1000
    
    # About 50% positive, 30% negative, 20% neutral
    positive_count = int(num_reviews * 0.5)
    negative_count = int(num_reviews * 0.3)
    neutral_count = num_reviews - positive_count - negative_count
    
    # Generate all the items to process
    items_to_process = []
    
    # Add positive reviews
    for _ in range(positive_count):
        customer_id = random.choice(customer_ids)
        product_id = f"PROD-{random.choice('ABCDEFGHIJK')}{random.randint(1, 10)}"
        date = generate_random_date()
        # Make 80% of reviews English
        language = "english" if random.random() < 0.8 else random.choice(LANGUAGES[1:])  # Skip "english" in the list
        items_to_process.append({
            'customer_id': customer_id,
            'product_id': product_id,
            'date': date,
            'rating': random.randint(4, 5),  # 4-5 stars
            'tone': 'positive',
            'language': language
        })
    
    # Add negative reviews
    for _ in range(negative_count):
        customer_id = random.choice(customer_ids)
        product_id = f"PROD-{random.choice('LMNOPQRS')}{random.randint(1, 10)}"
        date = generate_random_date()
        # Make 80% of reviews English
        language = "english" if random.random() < 0.8 else random.choice(LANGUAGES[1:])
        items_to_process.append({
            'customer_id': customer_id,
            'product_id': product_id,
            'date': date,
            'rating': random.randint(1, 2),  # 1-2 stars
            'tone': 'negative',
            'language': language
        })
    
    # Add neutral reviews
    for _ in range(neutral_count):
        customer_id = random.choice(customer_ids)
        product_id = f"PROD-{random.choice('TUVWXYZ')}{random.randint(1, 10)}"
        date = generate_random_date()
        # Make 80% of reviews English
        language = "english" if random.random() < 0.8 else random.choice(LANGUAGES[1:])
        items_to_process.append({
            'customer_id': customer_id,
            'product_id': product_id,
            'date': date,
            'rating': 3,  # 3 stars
            'tone': 'neutral',
            'language': language
        })
    
    # Process the items
    if use_parallel:
        reviews = batch_process_generation(items_to_process, generator, process_review_item)
    else:
        for item in items_to_process:
            review = process_review_item(item, generator)
            reviews.append(review)
    
    # Sort by date for realistic timeline
    reviews.sort(key=lambda x: x["review_date"])
    
    # Generate sequential IDs
    for i, review in enumerate(reviews):
        review["review_id"] = f"REV-{i+1:03d}"
    
    return reviews

def generate_support_tickets(customers, generator, use_parallel=False) -> List[Dict[str, Any]]:
    """Generate support tickets with appropriate distribution of issue types."""
    tickets = []
    customer_ids = [customer["customer_id"] for customer in customers]
    
    # More customers should have more tickets
    num_tickets = min(len(customers) * 2, 1000)  # Cap at 1000
    
    # Distribution of ticket types
    technical_count = int(num_tickets * 0.4)
    billing_count = int(num_tickets * 0.3)
    account_count = int(num_tickets * 0.2)
    feature_count = num_tickets - technical_count - billing_count - account_count
    
    # Generate all the items to process
    items_to_process = []
    
    # Technical issues
    for _ in range(technical_count):
        customer_id = random.choice(customer_ids)
        date = generate_random_date()
        # Make 80% of tickets English
        language = "english" if random.random() < 0.8 else random.choice(LANGUAGES[1:])
        items_to_process.append({
            'customer_id': customer_id,
            'date': date,
            'category': 'technical',
            'language': language
        })
    
    # Billing issues
    for _ in range(billing_count):
        customer_id = random.choice(customer_ids)
        date = generate_random_date()
        # Make 80% of tickets English
        language = "english" if random.random() < 0.8 else random.choice(LANGUAGES[1:])
        items_to_process.append({
            'customer_id': customer_id,
            'date': date,
            'category': 'billing',
            'language': language
        })
    
    # Account issues
    for _ in range(account_count):
        customer_id = random.choice(customer_ids)
        date = generate_random_date()
        # Make 80% of tickets English
        language = "english" if random.random() < 0.8 else random.choice(LANGUAGES[1:])
        items_to_process.append({
            'customer_id': customer_id,
            'date': date,
            'category': 'account',
            'language': language
        })
    
    # Feature requests
    for _ in range(feature_count):
        customer_id = random.choice(customer_ids)
        date = generate_random_date()
        # Make 80% of tickets English
        language = "english" if random.random() < 0.8 else random.choice(LANGUAGES[1:])
        items_to_process.append({
            'customer_id': customer_id,
            'date': date,
            'category': 'feature-request',
            'language': language
        })
    
    # Process the items
    if use_parallel:
        tickets = batch_process_generation(items_to_process, generator, process_ticket_item)
    else:
        for item in items_to_process:
            ticket = process_ticket_item(item, generator)
            tickets.append(ticket)
    
    # Sort by date for realistic timeline
    tickets.sort(key=lambda x: x["ticket_date"])
    
    # Generate sequential IDs
    for i, ticket in enumerate(tickets):
        ticket["ticket_id"] = f"TICKET-{i+1:03d}"
    
    return tickets

def parse_args():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(description='Generate synthetic customer data')
    parser.add_argument('--num-records', type=int, default=10, help='Number of customer records to generate')
    parser.add_argument('--model', type=str, default='distilgpt2', 
                        choices=list(FAST_MODELS.keys()),
                        help='Small language model to use for text generation. Available options: ' + 
                             ', '.join(f"{k} ({v['description']})" for k, v in FAST_MODELS.items()))
    parser.add_argument('--use-templates-only', action='store_true', help='Use only templates for text generation (much faster)')
    parser.add_argument('--parallel', action='store_true', help='Use parallel processing for faster generation')
    parser.add_argument('--template-ratio', type=float, default=0.7, help='Ratio of template usage (0.0-1.0, higher = more templates = faster)')
    return parser.parse_args()

def main():
    """Main function to generate and save synthetic data."""
    start_time = time.time()
    args = parse_args()
    num_customers = args.num_records
    
    # Validate template ratio is between 0 and 1
    template_ratio = max(0.0, min(1.0, args.template_ratio))
    
    # Set the global template ratio
    globals()['TEMPLATE_RATIO'] = template_ratio
    
    print(f"Generating synthetic data for {num_customers} customers...")
    print(f"Using template ratio: {template_ratio} (higher = faster generation)")
    
    if args.use_templates_only:
        print("Using templates only mode (no LLM will be loaded)")
        generator = None
    else:
        # Show information about the selected model
        model_name = args.model
        model_info = FAST_MODELS.get(model_name, {"description": "Custom model"})
        print(f"Setting up text generation with model: {model_name}")
        print(f"Model description: {model_info['description']}")
        generator = setup_llm(args.use_templates_only, args.model)
    
    # Generate different data types
    customers = generate_customer_base(num_customers)
    
    print("Generating customer interactions...")
    interactions = generate_customer_interactions(customers, generator, args.parallel)
    print(f"Generated {len(interactions)} interactions")
    
    print("Generating product reviews...")
    reviews = generate_product_reviews(customers, generator, args.parallel)
    print(f"Generated {len(reviews)} reviews")
    
    print("Generating support tickets...")
    tickets = generate_support_tickets(customers, generator, args.parallel)
    print(f"Generated {len(tickets)} tickets")
    
    # Save the generated data
    print("Saving data to ./data...")
    save_customers(customers)
    save_interactions(interactions)
    save_reviews(reviews)
    save_tickets(tickets)
    
    print(f"Generated {len(interactions)} interactions, {len(reviews)} reviews, and {len(tickets)} tickets")
    end_time = time.time()
    print(f"Total execution time: {end_time - start_time:.2f} seconds")
    print("Done!")

def generate_random_date():
    """Generate a random date within the last 180 days."""
    days_ago = random.randint(1, 180)
    return (datetime.now() - timedelta(days=days_ago)).strftime("%Y-%m-%dT%H:%M:%S")

def process_interaction_item(item, generator):
    """Process a single interaction item."""
    customer_id = item['customer_id']
    tone = item['tone']
    date = item['date']
    
    # Add more specificity to the tone for more varied output
    specific_tones = {
        "positive": ["appreciative", "enthusiastic", "satisfied", "impressed", "thankful", "delighted", 
                    "loyal", "complimentary", "grateful", "optimistic", "pleased", "happy", "thrilled"],
        "negative": ["frustrated", "disappointed", "concerned", "irritated", "confused", "dissatisfied", 
                    "angry", "annoyed", "upset", "demanding", "impatient", "skeptical", "discouraged"],
        "neutral": ["informative", "curious", "inquiring", "constructive", "analytical", "practical", 
                   "matter-of-fact", "professional", "straightforward", "detailed", "inquisitive", "patient"]
    }
    
    # Create more specific context for the interaction
    specific_tone = random.choice(specific_tones.get(tone, [tone]))
    
    # Randomize agent IDs more widely
    agent_id = f"AG-{random.randint(1, 100):03d}"
    
    # Add more variety to interaction types
    extended_interaction_types = INTERACTION_TYPES + ["video-call", "support-portal", "SMS", "mobile-app", "feedback-form", 
                                                      "support-chat", "knowledge-base", "community-forum", "social-media", 
                                                      "in-person", "scheduled-call", "web-demo", "consultation"]
    interaction_type = random.choice(extended_interaction_types)
    
    # Create various specific contexts for interactions
    contexts = [
        "enterprise", "small business", "non-profit", "educational", "technical", "non-technical", "new", "long-term",
        "international", "remote", "startup", "healthcare", "financial", "government", "retail", "manufacturing"
    ]
    
    context = random.choice(contexts)
    
    # Create various scenarios
    scenarios = [
        "having trouble with setup", "requesting a refund", "asking how to use advanced features", 
        "reporting a bug", "requesting documentation", "inquiring about pricing", "upgrading their account",
        "asking about compatibility", "struggling with integration", "needing help with customization",
        "exploring new features", "facing performance issues", "requiring assistance with migration",
        "following up on previous issues", "requesting training", "providing feedback"
    ]
    
    scenario = random.choice(scenarios)
    
    # Create various products
    products = [
        "CRM software", "analytics platform", "billing system", "cloud storage solution", "marketing automation tool",
        "project management tool", "database service", "communication platform", "security suite", "productivity app",
        "customer support system", "content management system", "e-commerce platform", "inventory management system",
        "HR software", "accounting software", "data visualization tool", "mobile application", "API service"
    ]
    
    product = random.choice(products)
    
    # Create the interaction with more specific tone for more varied outputs
    interaction = {
        "customer_id": customer_id,
        "interaction_date": date,
        "interaction_notes": generate_text_with_llm(generator, "interaction", specific_tone, context=context, scenario=scenario, product=product),
        "agent_id": agent_id,
        "interaction_type": interaction_type,
        "specific_tone": specific_tone,
        "context": context,
        "scenario": scenario,
        "product": product
    }
    return interaction

def process_review_item(item, generator):
    """Process a single review item."""
    customer_id = item['customer_id']
    product_id = item['product_id']
    date = item['date']
    rating = item['rating']
    tone = item['tone']
    language = item['language']
    
    # Add more specificity to the tone for more varied output
    specific_tones = {
        "positive": ["enthusiastic", "satisfied", "impressed", "excited", "grateful", "delighted",
                     "thrilled", "amazed", "pleased", "appreciative", "supportive", "content", "approving"],
        "negative": ["disappointed", "frustrated", "annoyed", "unhappy", "regretful", "critical",
                     "dissatisfied", "upset", "angry", "disillusioned", "skeptical", "dubious", "concerned"],
        "neutral": ["balanced", "objective", "analytical", "factual", "thoughtful", "moderate",
                    "fair", "unbiased", "honest", "practical", "realistic", "reasonable", "mixed"]
    }
    
    # Create more specific context for the review
    specific_tone = random.choice(specific_tones.get(tone, [tone]))
    
    # Add more varied rating nuances
    # Slightly vary the ratings within their general sentiment category
    if tone == "positive" and rating == 5 and random.random() < 0.4:
        rating = 4  # Some positive reviews are 4 stars
    elif tone == "positive" and rating == 4 and random.random() < 0.2:
        rating = 5  # Some 4-star reviews might be 5 stars
    elif tone == "negative" and rating == 1 and random.random() < 0.4:
        rating = 2  # Some negative reviews are 2 stars
    elif tone == "negative" and rating == 2 and random.random() < 0.2:
        rating = 1  # Some 2-star reviews might be 1 star
    elif tone == "neutral" and random.random() < 0.3:
        rating = random.choice([2, 4])  # Some neutral reviews lean slightly positive or negative
    
    # Create various contexts
    contexts = [
        "personal", "professional", "technical", "non-technical", "enterprise", "small business", 
        "power user", "casual user", "beginner", "expert", "frequent", "occasional", "returning",
        "international", "academic", "creative", "administrative", "management", "remote", "on-site"
    ]
    
    context = random.choice(contexts)
    
    # Create various products
    products = [
        "CRM software", "analytics platform", "billing system", "cloud storage solution", "marketing automation tool",
        "project management tool", "database service", "communication platform", "security suite", "productivity app",
        "customer support system", "content management system", "e-commerce platform", "inventory management system",
        "HR software", "accounting software", "data visualization tool", "mobile application", "API service",
        "collaboration tool", "video conferencing system", "learning management system", "design software"
    ]
    
    product = random.choice(products)
    
    # Create the review with more specific tone for more varied outputs
    review = {
        "customer_id": customer_id,
        "product_id": product_id,
        "review_date": date,
        "review_rating": rating,
        "review_text": generate_text_with_llm(generator, "review", specific_tone, language, context=context, product=product),
        "review_language": language,
        "specific_tone": specific_tone,
        "context": context,
        "product": product
    }
    return review

def process_ticket_item(item, generator):
    """Process a single ticket item."""
    customer_id = item['customer_id']
    date = item['date']
    category = item['category']
    language = item['language']
    
    # Add more specificity to the ticket category for more varied output
    specific_categories = {
        "technical": ["software-bug", "installation-issue", "performance-problem", "compatibility", "error-message", "crash",
                      "data-corruption", "sync-failure", "connection-issue", "timeout", "memory-leak", "version-conflict",
                      "resource-exhaustion", "security-vulnerability", "api-error", "authentication-failure"],
        "billing": ["payment-failed", "overcharge", "refund-request", "subscription-issue", "invoice-question", "discount-inquiry",
                    "auto-renewal", "upgrade-pricing", "downgrade-request", "payment-method-update", "tax-exemption", 
                    "promotional-code", "pricing-discrepancy", "cancellation-fee", "billing-cycle-change"],
        "account": ["login-problem", "access-denied", "profile-update", "password-reset", "account-migration", "permissions",
                    "two-factor-authentication", "account-verification", "email-update", "inactive-account", "account-merge",
                    "user-role-change", "deletion-request", "privacy-settings", "notification-preferences"],
        "feature-request": ["new-functionality", "integration-request", "usability-enhancement", "performance-improvement", "UI-suggestion",
                           "mobile-adaptation", "automation-capability", "reporting-enhancement", "workflow-optimization",
                           "accessibility-improvement", "localization-request", "security-enhancement", "data-export"]
    }
    
    # Get a more specific sub-category
    specific_category = category
    if category in specific_categories:
        specific_category = random.choice(specific_categories[category])
    
    # Determine status based on age of ticket
    days_ago = (datetime.now() - datetime.strptime(date, "%Y-%m-%dT%H:%M:%S")).days
    
    # Add variation to statuses
    extended_statuses = ["open", "in-progress", "resolved", "closed", "escalated", 
                         "pending-customer", "awaiting-approval", "on-hold", "reopened",
                         "pending-investigation", "in-review", "scheduled", "awaiting-deployment",
                         "needs-information", "transferred", "archived", "critical"]
    
    if days_ago < 7:
        # Newer tickets - use first part of status list
        status_options = extended_statuses[:8]
    elif days_ago < 30:
        # Medium-aged tickets - use middle part
        status_options = extended_statuses[2:12]
    else:
        # Older tickets - more likely resolved/closed
        status_options = extended_statuses[2:] + ["resolved", "closed"] * 3  # Weight toward resolved/closed
    
    status = random.choice(status_options)
    
    # Create various scenarios for tickets
    scenarios = [
        "after the recent update", "during peak usage", "when integrating with third-party services", 
        "while onboarding new users", "when scaling our operations", "during data migration",
        "in our production environment", "with large datasets", "on mobile devices", "with certain browsers",
        "under high loads", "during the checkout process", "with international customers",
        "with custom configurations", "when using advanced features", "during real-time operations"
    ]
    
    scenario = random.choice(scenarios)
    
    # Create various products
    products = [
        "CRM software", "analytics platform", "billing system", "cloud storage solution", "marketing automation tool",
        "project management tool", "database service", "communication platform", "security suite", "productivity app",
        "customer support system", "content management system", "e-commerce platform", "inventory management system",
        "HR software", "accounting software", "data visualization tool", "mobile application", "API service",
        "collaboration tool", "video conferencing system", "learning management system", "design software"
    ]
    
    product = random.choice(products)
    
    # Create various contexts
    contexts = [
        "enterprise", "small business", "non-profit", "educational", "technical", "non-technical", 
        "international", "remote", "startup", "healthcare", "financial", "government", "retail", 
        "manufacturing", "IT", "marketing", "sales", "developer", "designer", "executive"
    ]
    
    context = random.choice(contexts)
    
    # Create the ticket with more specific category for more varied outputs
    ticket = {
        "customer_id": customer_id,
        "ticket_date": date,
        "ticket_description": generate_text_with_llm(generator, "ticket", specific_category, language, context=context, scenario=scenario, product=product),
        "ticket_status": status,
        "ticket_category": category,
        "ticket_subcategory": specific_category,
        "context": context,
        "scenario": scenario,
        "product": product
    }
    return ticket

def batch_process_generation(items, generator, process_func, batch_size=10):
    """Process items in parallel batches using the generator."""
    results = []
    
    # Determine optimal number of workers
    max_workers = min(multiprocessing.cpu_count(), 4)  # Use at most 4 workers
    
    # Process in batches using thread pool
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = []
        for item in items:
            # Create a partial function with the item and generator
            future = executor.submit(process_func, item, generator)
            futures.append(future)
        
        # Retrieve results as they complete
        for future in futures:
            try:
                result = future.result()
                results.append(result)
            except Exception as e:
                print(f"Error processing item: {e}")
    
    return results

def save_customers(customers):
    """Save customer data to a JSON file."""
    with open(os.path.join('./data', 'customers.json'), 'w') as f:
        json.dump(customers, f, indent=2)

def save_interactions(interactions):
    """Save interaction data to a JSON file."""
    with open(os.path.join('./data', 'customer_interactions.json'), 'w') as f:
        json.dump(interactions, f, indent=2)
        
def save_reviews(reviews):
    """Save review data to a JSON file."""
    with open(os.path.join('./data', 'product_reviews.json'), 'w') as f:
        json.dump(reviews, f, indent=2)
        
def save_tickets(tickets):
    """Save ticket data to a JSON file."""
    with open(os.path.join('./data', 'support_tickets.json'), 'w') as f:
        json.dump(tickets, f, indent=2)

if __name__ == "__main__":
    main()