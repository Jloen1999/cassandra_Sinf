//package unex.dao;
//
//import com.datastax.oss.driver.api.core.CqlSession;
//import com.datastax.oss.driver.api.mapper.MapperBuilder;
//import unex.model.Destinos;
//
//import java.util.UUID;
//
//public class DestinoDaoImpl implements DestinoDao {
//    private final DestinoMapper destinoMapper;
//    private final DestinoDao destinoDao;
//
//    public DestinoDaoImpl(CqlSession session) {
//        MapperBuilder<DestinoMapper> builder = MapperBuilder.("application.conf");
//        this.destinoMapper = new MapperBuilder<DestinoMapper>(session).build();
//        this.destinoDao = destinoMapper.destinoDao();
//    }
//
//    @Override
//    public void insert(Destinos destino) {
//        destinoDao.insert(destino);
//    }
//
//    @Override
//    public Destinos findById(UUID destinoId) {
//        return destinoDao.findById(destinoId);
//    }
//
//    @Override
//    public void update(Destinos destino) {
//        destinoDao.update(destino);
//    }
//
//    @Override
//    public void delete(UUID destinoId) {
//        destinoDao.delete(destinoId);
//    }
//}
