module.exports = {
    "name": "options",
    "type": "object",
    "properties": {
        "job": {
            "type": "object",
            "properties": {
                "type": {
                    "type": "string",
                    "description": "the job type"
                }
            },
            "required": [
                "type"
            ]
        },
        "setting": {
            "type": "object",
            "properties": {
                "prefix": {
                    "type": "string",
                    "default": "jobs",
                    "description": "prefix for all queue keys"
                },
                "redis": {
                    "type": "object",
                    "properties": {
                        "host": {
                            "type": "string",
                            "default": "localhost"
                        },
                        "port": {
                            "anyOf": [
                                {
                                    "type": [
                                        "integer",
                                        "string"
                                    ]
                                }
                            ],
                            "default": 6379
                        }
                    }
                }
            }
        }
    }
}