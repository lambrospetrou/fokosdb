# Deletes all Partition DOs created during the test, including any children created by splits.
# DELETE {{base_url}}/api/databases/k6_db_root10_100mb
# {
#   "partitionOptions": {
#     "rootTreesN": 10,
#     "hashSplitN": 4,
#     "rangeSplitN": 4,
#     "hashSplitConditions": {
#       "maxSizeMb": 100
#     }
#   }
# }
# HTTP 200
