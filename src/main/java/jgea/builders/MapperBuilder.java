package jgea.builders;

import io.github.ericmedvet.jnb.core.Cacheable;
import io.github.ericmedvet.jnb.core.Discoverable;
import jgea.mappers.Mapper;

@Discoverable(prefixTemplate = "silvia.mapper")
public class MapperBuilder {

    private MapperBuilder() {
    }

    // Create a Mapper
    @SuppressWarnings("unused")
    @Cacheable
    public static Mapper treeToQueryMapper(){
        return new Mapper();
    }
}