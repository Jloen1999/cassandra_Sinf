//package unex.dao;
//
//import com.datastax.oss.driver.api.core.CqlSession;
//import com.datastax.oss.driver.api.mapper.annotations.*;
//import unex.model.DestinoDaoImpl;
//import unex.model.Destinos;
//
//import java.util.UUID;
//
//@Mapper
//public interface DestinoDao {
//
//    @DaoFactory
//    static DestinoDao destinoDao(CqlSession session) {
//        return new DestinoDaoImpl(session, destinoDao);
//    }
//
//    @Insert
//    void insert(Destinos destino);
//
//    @Query("SELECT * FROM destinos WHERE destino_id = :destinoId")
//    Destinos findById(UUID destinoId);
//
//    @Update
//    void update(Destinos destino);
//
//    @Query("DELETE FROM destinos WHERE destino_id = :destinoId")
//    void delete(UUID destinoId);
//}
