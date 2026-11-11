namespace NServiceBus.AcceptanceTests;

using AcceptanceTesting;
using EndpointTemplates;
using Features;
using NUnit.Framework;

public class When_subscribing_to_multiple_event_types : NServiceBusAcceptanceTest
{
    [Test]
    [Repeat(5)]
    public async Task Should_receive_both_concurrently_published_events()
    {
        var context = await Scenario.Define<Context>()
            .WithEndpoint<Publisher>(b => b.When(c => c.Subscribed, session => Task.WhenAll(
                session.Publish(new FirstEvent()),
                session.Publish(new SecondEvent()))))
            .WithEndpoint<Subscriber>(b => b.When(async (session, c) =>
            {
                await session.Subscribe<FirstEvent>();
                await session.Subscribe<SecondEvent>();
                c.Subscribed = true;
            }))
            .Done(c => c.ReceivedFirst && c.ReceivedSecond)
            .Run();

        using (Assert.EnterMultipleScope())
        {
            Assert.That(context.ReceivedFirst, Is.True);
            Assert.That(context.ReceivedSecond, Is.True);
        }
    }

    public class Context : ScenarioContext
    {
        public bool Subscribed { get; set; }
        public bool ReceivedFirst { get; set; }
        public bool ReceivedSecond { get; set; }
    }

    public class Publisher : EndpointConfigurationBuilder
    {
        public Publisher() => EndpointSetup<DefaultServer>();
    }

    public class Subscriber : EndpointConfigurationBuilder
    {
        public Subscriber() => EndpointSetup<DefaultServer>(c => c.DisableFeature<AutoSubscribe>());

        public class Handler(Context testContext) : IHandleMessages<FirstEvent>, IHandleMessages<SecondEvent>
        {
            public Task Handle(FirstEvent message, IMessageHandlerContext context)
            {
                testContext.ReceivedFirst = true;
                return Task.CompletedTask;
            }

            public Task Handle(SecondEvent message, IMessageHandlerContext context)
            {
                testContext.ReceivedSecond = true;
                return Task.CompletedTask;
            }
        }
    }

    public class FirstEvent : IEvent;
    public class SecondEvent : IEvent;
}
