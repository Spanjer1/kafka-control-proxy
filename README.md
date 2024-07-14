# Kafka Control Proxy: Protocol-Level Kafka Controls in a Customizable Proxy

## What is Kafka Control Proxy (KCP)?
KCP is a hackable and reactive intermediate layer to control Kafka traffic. It is designed to be a lightweight and flexible proxy that can be used to enforce Kafka protocol-level controls. 
It allows you to intercept, modify, and block Kafka requests and responses based on your own rules. Without comprising the performance, KCP can be used to enforce security, compliance, and operational policies on Kafka traffic.

## Why Kafka Control Proxy?
Kafka is a distributed streaming platform that is widely used for building real-time data pipelines and streaming applications. However, Kafka does not provide a built-in mechanism to enforce protocol-level controls.
There are some commercial solutions that provide protocol-level controls for Kafka, but they are expensive and not customizable. 
KCP is designed to be a lightweight and customizable alternative to these commercial solutions.

Some things you can do with:
- Software firewall for Kafka, e.g., block requests from a specific IP address based on the request content, authenticated user, or Kafka APIKey.
- Enforce security policies, e.g., block requests that contain sensitive information.
- Encrypt event fields in Kafka messages, only allow specific consumer to read certain fields.
- Topic management, naming standards, settings, and access control.
- Schema validation on complete messages.
- Implement chaos testing, mimic unstable network conditions, e.g., delay, drop, or reorder messages and see how your application responds to that. 

## How Kafka Control Proxy Works?


## Performance


## Getting Started
