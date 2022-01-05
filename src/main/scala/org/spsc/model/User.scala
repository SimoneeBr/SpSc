package org.spsc.model

case class User(created_at: String,
                id: String,
                description: String,
                public_metrics: UserPublicMetric,
                protect: Boolean,
                location: String,
                name: String,
                verified: Boolean,
                username: String,
                pinned_tweet_id: String,
                entities: Array[UserEntity],
                url: String,
                profile_image_url: String)

case class UserPublicMetric(followers_count: Int,
                            following_count: Int,
                            tweet_count: Int,
                            listed_count: Int)

case class UserEntity(url: UserUrl, description: UserDescription)

case class UserUrl(urls: Array[Url])

case class UserDescription(hashtags: Array[Hashtag],
                           mentions: Array[Mention],
                           urls: Array[Url])