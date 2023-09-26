"use strict";
const { 
    DynamoDBClient, PutItemCommand, GetItemCommand, QueryCommand, UpdateItemCommand, DeleteItemCommand,ScanCommand
} = require('@aws-sdk/client-dynamodb');

class DynamodbService {
    constructor(table_name) {
        this.client = new DynamoDBClient({ region: process.env.REGION });
        this.table = table_name;
        this.NO_FORMATE_DB_PROPS = process.env.NO_FORMATE_DB_PROPS ? new Set(JSON.parse(process.env.NO_FORMATE_DB_PROPS)) : new Set();
    }

    async save(params, conditional_attribute) {
        const input = {
            TableName: this.table,
            Item: {
                ...this._buildItems(params),
                "created_at": { S: new Date().toLocaleString() },
                "deleted": { S: '0' }
            },
            ConditionExpression: `attribute_not_exists(${conditional_attribute})`
        };
        const command = new PutItemCommand(input);
        return await this.client.send(command);
    }

    async update(keys = {}, params, conditional_attribute) {
        let attributesName = { '#updated_at': 'updated_at' };
        let attributesValues = { ':updated_at': { S: new Date().toLocaleString() } };
        let expressions = 'SET ';
        Object.keys(params).forEach(key => {
            attributesName[`#${key}`] = key;
            if (params[key] && typeof params[key] == 'boolean') attributesValues[`:${key}`] = { BOOL: params[key] };
            if (params[key] && typeof params[key] == 'number') attributesValues[`:${key}`] = { N: `${params[key]}` };
            else attributesValues[`:${key}`] = { S: typeof params[key] == 'string' ? `${params[key]}` : `${JSON.stringify(params[key])}` };
            expressions += `#${key} = :${key},`;
        });
        const command = new UpdateItemCommand({
            ExpressionAttributeNames: attributesName,
            ExpressionAttributeValues: attributesValues,
            Key: this._buildItems(keys),
            ReturnValues: "ALL_NEW",
            ConditionExpression: `attribute_exists(${conditional_attribute})`,
            TableName: this.table,
            UpdateExpression: `${expressions} #updated_at = :updated_at`
        });
        const { $metadata, Attributes } = await this.client.send(command);
        if ($metadata) return $metadata;
        else throw Error('Update failure');
        // if ($metadata.httpStatusCode == 200) return Attributes ? {$metadata, Item: this._sanitizeItem(Attributes)} : $metadata;
        // else throw Error('Update failure');
    }

    async delete(keys = {}) {
        const params = {
            Key: this._buildItems(keys),
            TableName: this.table
        }
        const command = new DeleteItemCommand(params);
        return await this.client.send(command);
    }

    async findById(keys = {}) {
        const params = {
            TableName: this.table,
            Key: this._buildItems(keys)
        };
        const command = new GetItemCommand(params);
        const response = await this.client.send(command);
        if (response.Item) return this._sanitizeItem(response.Item);
        else return {}
    }

    async findAll(filters = [], operator = "AND", limit) {
        const params = {
            TableName: this.table,
            "ReturnConsumedCapacity": "TOTAL"
        };
        if (filters.length > 0) {
            let filter_expression = "";
            let expression_attribute_values = {};
            filters.forEach((filter, index) => {
                Object.keys(filter).forEach((key) => {
                    filter_expression += ` ${key} = :${key}${index} ${operator}`;
                    expression_attribute_values[`:${key}${index}`] = this._buildValue(key, filter[key]);
                });
            });
            params['FilterExpression'] = filter_expression.replace(new RegExp(`${operator}$`), '');
            params['ExpressionAttributeValues'] = expression_attribute_values;
        }
        if (limit) params['Limit'] = limit;
        const command = new ScanCommand(params);
        const response = await this.client.send(command);
        if (response.Count > 0) return response.Items.map(item => this._sanitizeItem(item));
        else return [];
    }

    _sanitizeItem(item) {
        const item_object = item;
        const keys = Object.keys(item);
        let sanitized_item = {};
        keys.forEach(key => {
            const type = Object.keys(item_object[key])[0];
            sanitized_item[key] = item_object[key][type];
        });
        return sanitized_item;
    }

    _buildItems(params) {
        const items = {};
        Object.keys(params).forEach(key => { items[key] = this._buildValue(key, params[key]); });
        return items;
    }

    _buildValue(key, value) {
        if (value && typeof value == 'boolean') return { BOOL: value };
        if (value && typeof value == 'number') return { N: `${value}` };
        else return { S: typeof value == 'string' ? this._formatValue(key, value) : `${JSON.stringify(value)}` };
    }

    _formatValue(key, value) {
        const _value = value;
        const _key = key;
        if (!this.NO_FORMATE_DB_PROPS.has(_key.toLowerCase())) return _value.toLowerCase();
        else return _value;
    }

}

module.exports = DynamodbService;