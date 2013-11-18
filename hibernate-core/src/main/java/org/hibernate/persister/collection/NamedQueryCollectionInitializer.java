/*
 * Hibernate, Relational Persistence for Idiomatic Java
 *
 * Copyright (c) 2008, Red Hat Middleware LLC or third-party contributors as
 * indicated by the @author tags or express copyright attribution
 * statements applied by the authors.  All third-party contributions are
 * distributed under license by Red Hat Middleware LLC.
 *
 * This copyrighted material is made available to anyone wishing to use, modify,
 * copy, or redistribute it subject to the terms and conditions of the GNU
 * Lesser General Public License, as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY
 * or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser General Public License
 * for more details.
 *
 * You should have received a copy of the GNU Lesser General Public License
 * along with this distribution; if not, write to:
 * Free Software Foundation, Inc.
 * 51 Franklin Street, Fifth Floor
 * Boston, MA  02110-1301  USA
 *
 */
package org.hibernate.persister.collection;
import java.io.Serializable;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.hibernate.FlushMode;
import org.hibernate.HibernateException;
import org.hibernate.collection.spi.PersistentCollection;
import org.hibernate.engine.spi.CollectionKey;
import org.hibernate.engine.spi.SessionImplementor;
import org.hibernate.internal.AbstractQueryImpl;
import org.hibernate.internal.CoreMessageLogger;
import org.hibernate.loader.collection.CollectionInitializer;
import org.jboss.logging.Logger;

/**
 * A wrapper around a named query.
 * @author Gavin King
 */
public final class NamedQueryCollectionInitializer implements CollectionInitializer {

    private static final CoreMessageLogger LOG = Logger.getMessageLogger(CoreMessageLogger.class,
                                                                       NamedQueryCollectionInitializer.class.getName());

    private final String queryName;
	private final CollectionPersister persister;

	public NamedQueryCollectionInitializer(String queryName, CollectionPersister persister) {
		super();
		this.queryName = queryName;
		this.persister = persister;
	}

	@Override
  public void initialize(Serializable key, SessionImplementor session)
	throws HibernateException {

        LOG.debugf("Initializing collection: %s using named query: %s", persister.getRole(), queryName);

		//TODO: is there a more elegant way than downcasting?
		AbstractQueryImpl query = (AbstractQueryImpl) session.getNamedSQLQuery(queryName);
		if ( query.getNamedParameters().length>0 ) {
			query.setParameter(
					query.getNamedParameters()[0],
					key,
					persister.getKeyType()
				);
		}
		else {
			query.setParameter( 0, key, persister.getKeyType() );
		}
    List<?> list = query.setCollectionKey(key)
				.setFlushMode( FlushMode.MANUAL )
				.list();

    // See HHH-3273
    // Uh, how 'bout we save the collection for later retrieval?
    CollectionKey collectionKey = new CollectionKey(persister, key, persister.getOwnerEntityPersister().getEntityMode());
    Set keySet = new HashSet(session.getPersistenceContext().getCollectionsByKey().keySet());
    for (Object object : keySet) {
      if (collectionKey.equals(object)) {
        PersistentCollection persistentCollection = session.getPersistenceContext().getCollection(collectionKey);
        Serializable[] serializables = new Serializable[list.size()];
        for (int i = 0; i < list.size(); i++) {
          serializables[i] = persister.getElementType().disassemble(list.get(i), session, persistentCollection.getOwner());
        }
        persistentCollection.initializeFromCache(persister, serializables, persistentCollection.getOwner());
        persistentCollection.setSnapshot(key, persistentCollection.getRole(), serializables);
        persistentCollection.afterInitialize();
        session.getPersistenceContext().getCollectionEntry(persistentCollection).postInitialize(persistentCollection);
      }
    }

	}
}