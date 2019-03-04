/***************************** BEGIN LICENSE BLOCK ***************************

The contents of this file are subject to the Mozilla Public License, v. 2.0.
If a copy of the MPL was not distributed with this file, You can obtain one
at http://mozilla.org/MPL/2.0/.

Software distributed under the License is distributed on an "AS IS" basis,
WITHOUT WARRANTY OF ANY KIND, either express or implied. See the License
for the specific language governing rights and limitations under the License.
 
Copyright (C) 2019 Sensia Software LLC. All Rights Reserved.
 
******************************* END LICENSE BLOCK ***************************/

package org.sensorhub.api.event;

import java.util.Collection;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Flow.Subscriber;
import java.util.concurrent.Flow.Subscription;
import java.util.function.Consumer;
import java.util.function.Predicate;
import org.sensorhub.api.common.Event;
import org.sensorhub.api.common.IEventListener;


/**
 * <p>
 * Builder interface used to create new subscriptions for receiving events
 * from an {@link IEventBus event bus}.
 * </p>
 *
 * @author Alex Robin
 * @param <E> type of event handled by this builder
 * @date Mar 2, 2019
 */
public interface ISubscriptionBuilder<E extends Event>
{
    
    /**
     * Add one or more sources to the subscription<br/>
     * The source ID can also contain a trailing wildcard (e.g. "mysource/*")
     * @param sourceIDs IDs of sources to subscribe to
     * @return this builder for chaining
     */
    ISubscriptionBuilder<E> withSourceID(String... sourceIDs);
    
    
    /**
     * Add one or more sources to the subscription<br/>
     * The source ID can also contain a trailing wildcard (e.g. "mysource/*")
     * @param sourceIDs collection containing IDs of sources to subscribe to
     * @return this builder for chaining
     */
    ISubscriptionBuilder<E> withSourceID(Collection<String> sourceIDs);
    
    
    /**
     * Add one or more sources to the subscription.
     * @param sources
     * @return this builder for chaining
     */
    ISubscriptionBuilder<E> withSource(IEventProducer... sources);
    
    
    /**
     * Include only events of the specified types
     * @param types one or more accepted event types/classes
     * @return @return this builder for chaining
     */
    ISubscriptionBuilder<E> withEventType(@SuppressWarnings("unchecked") Class<? extends E>... types);
    
    
    /**
     * Filter events using a custom predicate
     * @param filter event filter predicate
     * @return this builder for chaining
     */
    ISubscriptionBuilder<E> withFilter(Predicate<? super E> filter);
    
    
    /**
     * Subscribe asynchronously with a reactive stream subscriber that allows
     * controlling the flow of the subscription (i.e. by applying back pressure) 
     * @param subscriber subscriber that will receive events
     * @return a future that will be notified when the subscription is ready
     */
    CompletableFuture<Subscription> withSubscriber(Subscriber<? super E> subscriber);
    
    
    /**
     * Subscribe asynchronously with a simple event listener without flow
     * control (back pressure) capability
     * @param listener listener that will receive the events
     * @return a future that will be notified when the subscription is ready
     */
    CompletableFuture<Subscription> withListener(IEventListener listener);
    
    
    /**
     * Subscribe asynchronously with a simple consumer without flow control
     * (back pressure) capability
     * @param onNext callback invoked every time a new event is available
     * @return a future that will be notified when the subscription is ready
     */
    CompletableFuture<Subscription> withConsumer(Consumer<? super E> onNext);
    
    
    /**
     * Subscribe asynchronously with a simple consumer without flow control
     * (back pressure) capability
     * @param onNext callback invoked every time a new event is available
     * @param onError callback invoked if an error occurs while delivering 
     * events to this subscription (e.g. the onNext callback throws an exception)
     * @return a future that will be notified when the subscription is ready
     */
    CompletableFuture<Subscription> withConsumer(Consumer<? super E> onNext, Consumer<Throwable> onError);
    
    
    /**
     * Subscribe asynchronously with a simple consumer without flow control
     * (back pressure) capability
     * @param onNext callback invoked every time a new event is available
     * @param onError callback invoked if an error occurs while delivering 
     * events to this subscription (e.g. the onNext callback throws an exception)
     * @param onComplete callback invoked when the subscription is closed by
     * the publisher (i.e. no more events will be delivered to this subscription)
     * @return a future that will be notified when the subscription is ready
     */
    CompletableFuture<Subscription> withConsumer(Consumer<? super E> onNext, Consumer<Throwable> onError, Runnable onComplete);
}