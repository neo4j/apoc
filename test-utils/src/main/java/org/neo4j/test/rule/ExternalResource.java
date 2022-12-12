package org.neo4j.test.rule;

import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;

/**
 * A better version of {@link org.junit.rules.ExternalResource} that properly handles exceptions in {@link
 * #after()}.
 */
public abstract class ExternalResource implements TestRule
{
    @Override
    public Statement apply( final Statement base, Description description )
    {
        return new Statement()
        {
            @Override
            public void evaluate() throws Throwable
            {
                before();
                Throwable failure = null;
                try
                {
                    base.evaluate();
                }
                catch ( Throwable e )
                {
                    failure = e;
                }
                finally
                {
                    try
                    {
                        after();
                    }
                    catch ( Throwable e )
                    {
                        if ( failure != null )
                        {
                            failure.addSuppressed( e );
                        }
                        else
                        {
                            failure = e;
                        }
                    }
                }
                if ( failure != null )
                {
                    throw failure;
                }
            }
        };
    }

    /**
     * Override to set up your specific external resource.
     */
    protected void before()
    {
        // do nothing
    }

    /**
     * Override to tear down your specific external resource.
     */
    protected void after()
    {
        // do nothing
    }
}
